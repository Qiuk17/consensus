package consensus

type ConsensusProvider interface {
	Reset()
	Propose(value uint64) error
	Commit() <-chan uint64
}
