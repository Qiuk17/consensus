package raft

import (
	"context"
	"fmt"
)

func (n *raftNode) handleAppendEntities(entities appendEntities, term uint64, role raftRole) appendEntitiesResponse {
	// Lock logs for reading if entities are empty.
	// Otherwise, lock logs for writing.
	// fmt.Printf("Node[%d] receive appendEntities from Node[%d]\n", n.uid, entities.leaderId)
	if len(entities.entities) == 0 {
		n.logsLock.RLock()
		defer n.logsLock.RUnlock()
	} else {
		n.logsLock.Lock()
		defer n.logsLock.Unlock()
	}
	// Update term and transform to follower when receiving a larger term.
	if entities.term > term {
		n.transformRoleAndLeaderId(role, follower, term, entities.term, entities.leaderId)
	} else if entities.term < term {
		// Reject the request if the term is smaller.
		return appendEntitiesResponse{
			term:    term,
			success: false,
		}
	} else {
		// Request term is equal to current term.
		// Update the leader id.
		n.roleCond.L.Lock()
		n.leaderId = entities.leaderId
		n.roleCond.L.Unlock()
	}
	// Request term is not smaller than current term.
	// Reset the heartbeat timer.
	n.receivedHeartbeat.Store(true)

	// Reject the request if the previous log term and index do not match.

	// `nextLogIndex` from leader is in range (0, len(n.logs)].
	// So, n.logs[entities.nextLogIndex-1] is always valid.
	if entities.nextLogIndex > 0 && entities.nextLogIndex <= len(n.logs) {
		// Reject the request if the previous log term does not match.
		if entities.prevLogTerm != n.logs[entities.nextLogIndex-1].term {
			// Delete the log entity and all the following entities.
			n.logs = n.logs[:entities.nextLogIndex-1]
			return appendEntitiesResponse{
				term:    term,
				success: false,
			}
		}
	} else if entities.nextLogIndex == 0 {
		// If `nextLogIndex` is 0, just clear the logs and ignore the previous log term and index.
		// The leader is requesting to append the first log entity.
		// The request is always accepted.
		n.logs = n.logs[:0]
	} else {
		// nextLogIndex is out of range.
		return appendEntitiesResponse{
			term:    term,
			success: false,
		}
	}

	// Request accepted.
	// Append the logs if the entities are not empty.
	if len(entities.entities) != 0 {
		n.logs = append(n.logs, entities.entities...)
	}

	// Update the committed size.
	if entities.leaderCommitSize > n.committedSize {
		newCommitSize := min(entities.leaderCommitSize, len(n.logs))
		for _, entity := range n.logs[n.committedSize:newCommitSize] {
			n.commitChannel <- entity
		}
		n.committedSize = newCommitSize
	}

	return appendEntitiesResponse{
		term:    term,
		success: true,
	}
}

func (n *raftNode) handleVoteRequest(request voteRequest, term uint64, role raftRole, votedFor uint64) voteResponse {
	// Update term and transform to follower when receiving a larger term.
	if request.term > term {
		n.transformRoleAndLeaderId(role, follower, term, request.term, 0)
	} else if request.term < term {
		// Reject the request if the term is smaller.
		return voteResponse{
			term:        term,
			voteGranted: false,
		}
	}
	// Request term is not smaller than current term.
	// Reset the heartbeat timer.
	n.receivedHeartbeat.Store(true)

	// Reject the request if the candidate's log is not up-to-date.
	n.logsLock.RLock()
	defer n.logsLock.RUnlock()
	if request.prevLogTerm < n.prevLogTermUnsafe(0) {
		return voteResponse{
			term:        term,
			voteGranted: false,
		}
	}
	if request.prevLogTerm == n.prevLogTermUnsafe(0) && request.nextLogIndex < len(n.logs) {
		return voteResponse{
			term:        term,
			voteGranted: false,
		}
	}

	// Grant the vote if not voted for any other candidate in this term.
	if votedFor == 0 || votedFor == request.candidateId {
		n.votedFor = request.candidateId
		fmt.Printf("Node[%d] vote for Node[%d] at term %d\n", n.uid, request.candidateId, term)
		return voteResponse{
			term:        term,
			voteGranted: true,
		}
	} else {
		return voteResponse{
			term:        term,
			voteGranted: false,
		}
	}
}

func (n *raftNode) handleProposeRequest(request proposeRequest, role raftRole, leaderId uint64) {
	// Forward the request to the leader if the node is not leader.
	if role != leader {
		_, cancel := n.network.Send(n.uid, leaderId, request)
		defer cancel()
		return
	}
	// Append the entity to the logs.
	n.logsLock.Lock()
	defer n.logsLock.Unlock()
	n.logs = append(n.logs, logEntity{
		term:  n.term,
		value: request.value,
	})
}

func (n *raftNode) Receive(message any, _ context.Context) (<-chan any, error) {
	// Freeze the current term and role.
	n.roleCond.L.Lock()
	term := n.term
	role := n.role
	votedFor := n.votedFor
	leaderId := n.leaderId
	n.roleCond.L.Unlock()
	responseChannel := make(chan any)
	go func() {
		defer close(responseChannel)
		switch message := message.(type) {
		case appendEntities:
			responseChannel <- n.handleAppendEntities(message, term, role)
		case voteRequest:
			responseChannel <- n.handleVoteRequest(message, term, role, votedFor)
		case proposeRequest:
			n.handleProposeRequest(message, role, leaderId)
			responseChannel <- nil
		default:
			panic("unknown message type")
		}
	}()
	return responseChannel, nil
}
