package raft

import (
	"consensus/consensus"
	"time"
)

const heartbeatInterval time.Duration = 70 * time.Millisecond

func (n *raftNode) sendHeartbeat(term uint64) {
	n.logsLock.RLock()
	// Check the latest log entity.
	heartbeat := appendEntities{
		term:             term,
		leaderId:         n.uid,
		nextLogIndex:     len(n.logs),
		prevLogTerm:      n.prevLogTermUnsafe(0),
		entities:         make([]logEntity, 0),
		leaderCommitSize: n.committedSize,
	}
	n.logsLock.RUnlock()
	// Send heartbeat parallel.
	for _, id := range n.otherNodesIds {
		go func(id uint64) {
			responseChannel, _ := consensus.Send[appendEntitiesResponse](n.network, n.uid, id, heartbeat)
			select {
			case response := <-responseChannel:
				if !response.success {
					if response.term > n.term {
						n.transformRole(leader, follower, n.term, response.term)
					} else {
						n.fixLogs(id)
					}
				}
			case <-time.After(1 * time.Second):
				// do nothing
			}
		}(id)
	}
}

func (n *raftNode) launchLeaderLoop() {
	go func() {
		// Run forever.
		for {
			// Wait until role is leader
			n.roleCond.L.Lock()
			for n.role != leader {
				n.roleCond.Wait()
			}
			term := n.term
			n.roleCond.L.Unlock()
			// Send heartbeats to all other nodes.
			n.sendHeartbeat(term)
			<-time.After(heartbeatInterval)
		}
	}()
}

func (n *raftNode) fixLogs(targetId uint64) {
	n.followersStatusLock.Lock()
	targetIdx := n.idToIdx[targetId]
	if n.followerFixing[targetIdx] {
		// The log is being fixed.
		n.followersStatusLock.Unlock()
		return
	}
	// Mark it as fixing.
	n.followerFixing[targetIdx] = true
	n.followersStatusLock.Unlock()
	// Unlock when finished.
	defer func() {
		n.followersStatusLock.Lock()
		n.followerFixing[targetIdx] = false
		n.followersStatusLock.Unlock()
	}()

	n.logsLock.RLock()
	defer n.logsLock.RUnlock()
	logsLen := len(n.logs)

	n.roleCond.L.Lock()
	term := n.term
	n.roleCond.L.Unlock()

	timeoutTimer := time.NewTimer(time.Second)
	defer timeoutTimer.Stop()

	for nextLogIdx := logsLen - 1; nextLogIdx >= 0; nextLogIdx-- {
		respChannel, cancel := consensus.Send[appendEntitiesResponse](n.network, n.uid, targetId, appendEntities{
			term:             term,
			leaderId:         n.uid,
			nextLogIndex:     nextLogIdx,
			prevLogTerm:      n.prevLogTermUnsafe(logsLen - nextLogIdx),
			leaderCommitSize: n.committedSize,
			entities:         n.logs[nextLogIdx:],
		})
		defer cancel()
		timeoutTimer.Reset(time.Second)
		select {
		case response := <-respChannel:
			if response.term > term {
				// Stop fixing, it will be fixed by the leader with greater term.
				return
			}
			if response.success {
				// Fixture done. Store nextLogIndex of this follower. Increase committed size if the majority of followers reaches it.
				n.followersStatusLock.Lock()
				n.nextLogIndexes[targetIdx] = len(n.logs)
				majority := (len(n.otherNodesIds)+1)/2 + 1
				for ; n.committedSize <= logsLen; n.committedSize++ {
					// Count if nextLogIndex reach the majority of this cluster.
					satisfied := 0
					for _, nextLogIdx := range n.nextLogIndexes {
						if nextLogIdx > n.committedSize {
							satisfied++
						}
					}
					if satisfied < majority {
						break
					}
				}
				n.followersStatusLock.Unlock()
				return
			}
			// Fix failed. Decrease and retry.
		case <-timeoutTimer.C:
			// Give up, automatically retry after the next successful heartbeat.
			return
		}
	}
}
