package paxos

import (
	"context"
	"sync"
)

type Acceptor struct {
	promisedProposalId uint64
	acceptedProposalId uint64
	acceptedValue      uint64

	lock sync.Mutex
}

func (a *Acceptor) init() {
	a.promisedProposalId = 0
	a.acceptedProposalId = 0
	a.acceptedValue = 0
}

func (a *Acceptor) Receive(message any, ctx context.Context) (<-chan any, error) {
	responseChannel := make(chan any)
	switch request := message.(type) {
	case PrepareRequest:
		go func() {
			responseChannel <- a.handlePrepare(request)
		}()
	case AcceptRequest:
		go func() {
			responseChannel <- a.handlerAccept(request)
		}()
	default:
		panic("error message type")
	}
	return responseChannel, nil
}

func (a *Acceptor) handlePrepare(request PrepareRequest) PrepareResponse {
	a.lock.Lock()
	defer a.lock.Unlock()
	// Acceptors should not promise when the proposal id is *not greater* than the previously promised one.
	if request.ProposalId <= a.promisedProposalId {
		return PrepareResponse{
			Promised: false,
		}
	}
	// Make a promise.
	a.promisedProposalId = request.ProposalId
	return PrepareResponse{
		Promised:           true,
		AcceptedProposalId: a.acceptedProposalId,
		AcceptedValue:      a.acceptedValue,
	}
}

func (a *Acceptor) handlerAccept(request AcceptRequest) AcceptResponse {
	a.lock.Lock()
	defer a.lock.Unlock()
	// Acceptors should not accept when the proposal id is *less* than the promised one.
	if request.ProposalId < a.promisedProposalId {
		return AcceptResponse{
			Accepted: false,
		}
	}
	// Accept otherwise.
	a.acceptedProposalId = request.ProposalId
	a.acceptedValue = request.Value
	return AcceptResponse{
		Accepted: true,
	}
}
