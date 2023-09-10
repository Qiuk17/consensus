package raft

import (
	"consensus/consensus"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

type raftRole uint8

const (
	follower raftRole = iota
	candidate
	leader
)

func (r raftRole) String() string {
	switch r {
	case follower:
		return "F"
	case candidate:
		return "C"
	case leader:
		return "L"
	default:
		return fmt.Sprintf("unknown role: %d", r)
	}
}

type logEntity struct {
	value uint64
	term  uint64
}

type raftNode struct {
	// node constant properties
	uid           uint64
	otherNodesIds []uint64
	idToIdx       map[uint64]int
	network       consensus.Network
	commitChannel chan logEntity
	// This channel is used to notify the primary commit index.
	// In practice, commits should be applied to state machines on all nodes.
	// To simplify and focus on the core algorithm, we only apply commits from leader to state machine.
	primaryCommitChannel chan<- uint64

	// raft common fields controlled by roleCond
	role     raftRole
	leaderId uint64
	term     uint64
	votedFor uint64
	roleCond *sync.Cond

	// log entries controlled by logsLock
	logs          []logEntity
	committedSize int
	logsLock      sync.RWMutex

	// leader specific fields controlled by followersStatusLock
	nextLogIndexes      []int
	followerFixing      []bool
	followersStatusLock sync.Mutex

	// follower specific fields
	receivedHeartbeat atomic.Bool
}

type Raft struct {
	nodes         []*raftNode
	network       consensus.Network
	commitChannel chan uint64
}

func (r Raft) Reset() {
	// Raft should not be reset.
}

func (r Raft) Propose(value uint64) error {
	responseChannel, cancel := r.network.Send(0, r.nodes[rand.Intn(len(r.nodes))].uid, proposeRequest{value: value})
	defer cancel()
	<-responseChannel
	return nil
}

func (r Raft) Commit() <-chan uint64 {
	return r.commitChannel
}

func NewRaft(nodeCounts int, network consensus.Network) *Raft {
	// Initialize nodes.
	uidGenerator := consensus.NewNaiveUidGenerator()
	nodes := make([]*raftNode, nodeCounts)
	commitChannel := make(chan uint64)
	ids := make([]uint64, nodeCounts)
	for i := 0; i < nodeCounts; i++ {
		ids[i] = uidGenerator.Next()
	}
	for i := 0; i < nodeCounts; i++ {
		otherNodes := make([]uint64, nodeCounts-1)
		// Remove the current node id from other nodes.
		copy(otherNodes, ids[:i])
		copy(otherNodes[i:], ids[i+1:])
		nodes[i] = newRaftNode(ids[i], otherNodes, network, commitChannel)
		network.Register(ids[i], nodes[i])
	}
	// Launch all nodes.
	for _, node := range nodes {
		node.launchLoops()
	}
	return &Raft{
		nodes:         nodes,
		network:       network,
		commitChannel: make(chan uint64),
	}
}

func newRaftNode(uid uint64, otherNodesIds []uint64, network consensus.Network, primaryCommitChannel chan<- uint64) *raftNode {
	idToIdx := make(map[uint64]int)
	for i, id := range otherNodesIds {
		idToIdx[id] = i
	}
	node := &raftNode{
		uid:                  uid,
		otherNodesIds:        otherNodesIds,
		idToIdx:              idToIdx,
		network:              network,
		commitChannel:        make(chan logEntity),
		primaryCommitChannel: primaryCommitChannel,
		role:                 follower,
		leaderId:             0,
		term:                 0,
		votedFor:             0,
		roleCond:             sync.NewCond(&sync.Mutex{}),
		logs:                 make([]logEntity, 0),
		committedSize:        0,
		logsLock:             sync.RWMutex{},
		nextLogIndexes:       make([]int, len(otherNodesIds)),
		followerFixing:       make([]bool, len(otherNodesIds)),
		followersStatusLock:  sync.Mutex{},
		receivedHeartbeat:    atomic.Bool{},
	}
	return node
}

func (n *raftNode) launchLoops() {
	n.launchLeaderLoop()
	n.launchCandidateLoop()
	n.launchFollowerLoop()
	n.launchCommitListener()
}

func (n *raftNode) launchCommitListener() {
	go func() {
		for committedEntity := range n.commitChannel {
			_, role := n.readTermAndRole()
			n.logsLock.RLock()
			fmt.Printf("Node[%d] commit <%d, %d>\n", n.uid, committedEntity.term, committedEntity.value)
			// Forward commits to the state machine if the node is leader.
			if role == leader {
				n.primaryCommitChannel <- uint64(committedEntity.value)
			}
			n.logsLock.RUnlock()
		}
	}()
}

func (n *raftNode) readTermAndRole() (uint64, raftRole) {
	n.roleCond.L.Lock()
	defer n.roleCond.L.Unlock()
	return n.term, n.role
}

func (n *raftNode) transformRole(old raftRole, new raftRole, oldTerm uint64, newTerm uint64) {
	n.transformRoleAndLeaderId(old, new, oldTerm, newTerm, 0)
}

func (n *raftNode) transformRoleAndLeaderId(old raftRole, new raftRole, oldTerm uint64, newTerm uint64, newLeaderId uint64) {
	n.roleCond.L.Lock()
	defer n.roleCond.L.Unlock()
	if n.role == old && n.term == oldTerm && newTerm >= oldTerm {
		fmt.Printf("Node[%d] transform from <%s, %d> to <%s, %d>\n", n.uid, old, oldTerm, new, newTerm)
		n.role = new
		n.term = newTerm
		if newLeaderId != 0 {
			n.leaderId = newLeaderId
		}
		// Clear votedFor if term is updated.
		n.votedFor = 0
		n.roleCond.Broadcast()
	}
}

// prevLogTermUnsafe returns the term of the log at revIndex before the last log.
func (n *raftNode) prevLogTermUnsafe(revIndex int) uint64 {
	l := len(n.logs)
	if l <= revIndex {
		return 0
	}
	return n.logs[l-(revIndex+1)].term
}

func randomDuration(base time.Duration, bias time.Duration) time.Duration {
	return base + time.Duration(rand.Int63n(int64(bias)))
}
