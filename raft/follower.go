package raft

import (
	"time"
)

const followerElectionTimeoutBase time.Duration = 150 * time.Millisecond
const followerElectionTimeoutBias time.Duration = 50 * time.Millisecond

// Followers are passive: they issue no RPCs on their own behalf,
// but instead respond to RPCs from candidates and leaders.

func (n *raftNode) launchFollowerLoop() {
	go func() {
		// Run forever.
		for {
			// Wait until role is follower
			n.roleCond.L.Lock()
			for n.role != follower {
				n.roleCond.Wait()
			}
			// Freeze the current term.
			term := n.term
			n.roleCond.L.Unlock()
			// Wait for a random time.
			<-time.After(randomDuration(followerElectionTimeoutBase, followerElectionTimeoutBias))
			// Advance to next term if no heartbeat is received.
			if n.receivedHeartbeat.Swap(false) {
				// do nothing
			} else {
				n.transformRole(follower, candidate, term, term+1)
			}
		}
	}()
}
