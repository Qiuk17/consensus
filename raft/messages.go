package raft

type appendEntities struct {
	term     uint64
	leaderId uint64

	nextLogIndex int
	prevLogTerm  uint64

	entities []logEntity

	leaderCommitSize int
}

type appendEntitiesResponse struct {
	term    uint64
	success bool
}

type voteRequest struct {
	term        uint64
	candidateId uint64

	nextLogIndex int
	prevLogTerm  uint64
}

type voteResponse struct {
	term        uint64
	voteGranted bool
}

type proposeRequest struct {
	value uint64
}
