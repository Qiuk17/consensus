package paxos

type ProposeNewValueRequest struct {
	NewValue uint64
}

type ProposeResult uint8

const (
	PrepareFailed ProposeResult = iota
	AcceptFailed
	Accepted
)

type ProposeNewValueResponse struct {
	Result ProposeResult
}

type PrepareRequest struct {
	ProposalId uint64
}

type PrepareResponse struct {
	Promised           bool
	AcceptedProposalId uint64
	AcceptedValue      uint64
}

type AcceptRequest struct {
	ProposalId uint64
	Value      uint64
}

type AcceptResponse struct {
	Accepted bool
}

type ChosenProposal struct {
	ProposalId uint64
	Value      uint64
}
