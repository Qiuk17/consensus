package raft

import (
	"consensus/consensus"
	"time"
)

const candidateElectionTimeoutBase time.Duration = 150 * time.Millisecond
const candidateElectionTimeoutBias time.Duration = 50 * time.Millisecond

func (n *raftNode) launchCandidateLoop() {
	go func() {
		// Run forever.
		for {
			// Wait until role is candidate
			n.roleCond.L.Lock()
			for n.role != candidate {
				n.roleCond.Wait()
			}
			// Freeze the current term.
			term := n.term
			// Vote for self.
			n.votedFor = n.uid
			n.roleCond.L.Unlock()
			// Send vote requests to all other nodes.
			n.sendVoteRequests(term)
			<-time.After(randomDuration(candidateElectionTimeoutBase, candidateElectionTimeoutBias))
			// Advance to next term.
			n.transformRole(candidate, candidate, term, term+1)
		}
	}()
}

func (n *raftNode) sendVoteRequests(term uint64) {
	voteChannel := make(chan bool)
	majority := uint32(len(n.otherNodesIds)+1)/2 + 1
	// Start a goroutine to count votes.
	go func() {
		voteCount := 0
		totalCount := 0
		for voted := range voteChannel {
			totalCount++
			if voted {
				voteCount++
				if voteCount >= int(majority) {
					n.transformRole(candidate, leader, term, term)
					return
				}
			}
		}
	}()
	// Vote for self.
	voteChannel <- true
	// Send vote requests parallel.
	for _, id := range n.otherNodesIds {
		go func(id uint64) {
			n.logsLock.RLock()
			responseChannel, _ := consensus.Send[voteResponse](n.network, n.uid, id, voteRequest{
				term:         term,
				candidateId:  n.uid,
				nextLogIndex: len(n.logs),
				prevLogTerm:  n.prevLogTermUnsafe(0),
			})
			n.logsLock.RUnlock()
			select {
			case response := <-responseChannel:
				if response.voteGranted {
					voteChannel <- true
				} else {
					if response.term > n.term {
						n.transformRole(candidate, follower, n.term, response.term)
					}
					voteChannel <- false
				}
			case <-time.After(150 * time.Millisecond):
				voteChannel <- false
			}
		}(id)
	}
}
