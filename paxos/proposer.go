package paxos

import (
	"consensus/consensus"
	"context"
	"fmt"
	"sync"
	"time"
)

type Proposer struct {
	uid                 uint64
	acceptorsIds        []uint64
	network             consensus.Network
	proposalIdGenerator consensus.UidGenerator

	// Only permits one propose procedure running at the same time.
	proposeLock sync.Mutex
	IsLocked    bool
	proposalId  uint64

	// Persist previously accepted proposal to restore an unfinished paxos.
	acceptedProposalLock sync.Mutex
	acceptedProposalId   uint64
	acceptedValue        uint64

	// The channel to receive chosen proposals.
	chosenChannel chan<- ChosenProposal
}

func NewProposer(uid uint64, acceptors []uint64, network consensus.Network, proposalIdGenerator consensus.UidGenerator, chosenChannel chan<- ChosenProposal) *Proposer {
	return &Proposer{
		uid:                 uid,
		acceptorsIds:        acceptors,
		network:             network,
		proposalIdGenerator: proposalIdGenerator,
		chosenChannel:       chosenChannel,
	}
}

// Receive only accepts requests from a client to start a new paxos instance.
func (p *Proposer) Receive(message any, _ context.Context) (<-chan any, error) {
	switch request := message.(type) {
	case ProposeNewValueRequest:
		resultChan := make(chan any)
		go func() {
			// Enter the propose-procedure.
			p.proposeLock.Lock()
			p.IsLocked = true
			defer func() {
				p.IsLocked = false
				p.proposeLock.Unlock()
			}()

			// Clear previously accepted proposal.
			p.acceptedProposalId = 0

			// Prepare
			if !p.Prepare() {
				resultChan <- ProposeNewValueResponse{
					Result: PrepareFailed,
				}
				return
			}

			// Accept and choose
			if !p.Accept(request.NewValue) {
				resultChan <- ProposeNewValueResponse{
					Result: AcceptFailed,
				}
			} else {
				resultChan <- ProposeNewValueResponse{
					Result: Accepted,
				}
			}
		}()
		return resultChan, nil
	default:
		return nil, fmt.Errorf("proposer[%d] cannot handle the message", p.uid)
	}
}

// Prepare is the first stage of basic paxos.
func (p *Proposer) Prepare() bool {
	// Generate a new proposal number.
	p.proposalId = p.proposalIdGenerator.Next()

	// Channels for signaling success or failure.
	success := make(chan struct{})
	failure := make(chan struct{})
	promiseChannel := make(chan bool)

	// Calculate the majority threshold.
	majority := len(p.acceptorsIds)/2 + 1
	failureMajority := len(p.acceptorsIds) - majority + 1

	// Start goroutines that close the success or failure channels when the corresponding barrier is reached.
	go func() {
		defer func() {
			// Recover from panic.
			recover()
		}()
		for result := range promiseChannel {
			if result {
				majority--
			} else {
				failureMajority--
			}
			if majority == 0 {
				close(success)
			} else if failureMajority == 0 {
				close(failure)
			}
		}

	}()

	// Construct prepare request.
	prepareRequest := PrepareRequest{
		ProposalId: p.proposalId,
	}

	// Send Prepare requests to all acceptors.
	for _, acceptorId := range p.acceptorsIds {
		go func(acceptorId uint64) {
			// Send request and get response.
			response, cancel := p.network.Send(p.uid, acceptorId, prepareRequest)
			defer cancel()

			// Wait for response or timeout.
			select {
			case message := <-response:
				// Process response
				switch response := message.(type) {
				case PrepareResponse:
					// If the promise is granted, signal success; otherwise, signal failure.
					if response.Promised {
						p.acceptedProposalLock.Lock()
						defer p.acceptedProposalLock.Unlock()
						if response.AcceptedProposalId > p.acceptedProposalId {
							p.acceptedProposalId = response.AcceptedProposalId
							p.acceptedValue = response.AcceptedValue
						}
						promiseChannel <- true
					} else {
						promiseChannel <- false
					}
				default:
					// Unexpected message type.
					panic("Unexpected message type in prepare stage")
				}
			case <-success:
				// When the proposer gets promises from the majority,
				// it is not necessary to care about pending requests.
				// fmt.Println("Proposer[", p.uid, "] ignored the prepare result from acceptor[", acceptorId, "]")
			case <-failure:
				// Same as `success`
				// fmt.Println("Proposer[", p.uid, "] ignored the prepare result from acceptor[", acceptorId, "]")
			case <-time.After(1 * time.Second):
				// If timeout, signal failure
				fmt.Println("Proposer[", p.uid, "] timed out waiting for PrepareResponse from Acceptor[", acceptorId, "]")
				promiseChannel <- false
			}
		}(acceptorId)
	}

	// Wait for either success or failure.
	select {
	case <-success:
		fmt.Println("Proposer[", p.uid, "] got majority of promises")
		return true
	case <-failure:
		// fmt.Println("Proposer[", p.uid, "] failed to get majority of promises")
		return false
	}
}

// Accept is the second stage of basic paxos.
// In this version of paxos, it will choose the value after received accepted response from the majority.
func (p *Proposer) Accept(userProposedValue uint64) bool {
	// Determine which value to accept.
	p.acceptedProposalLock.Lock()
	defer p.acceptedProposalLock.Unlock()
	value := userProposedValue
	// Restore an unfinished paxos.
	if p.acceptedProposalId != 0 {
		value = p.acceptedValue
	}

	// Calculate majority to choose proposal
	majority := len(p.acceptorsIds)/2 + 1

	// Accept event channel
	acceptChannel := make(chan bool)
	defer close(acceptChannel)
	// When the majority accept this proposal, it is chosen
	go func() {
		for accepted := range acceptChannel {
			if accepted {
				majority--
				// The proposal is accepted by the majority.
				if majority == 0 {
					p.chosenChannel <- ChosenProposal{
						ProposalId: p.proposalId,
						Value:      value,
					}
				}
			}
		}
	}()

	// Wait group to wait for all acceptors to respond or timeout
	wg := &sync.WaitGroup{}
	wg.Add(len(p.acceptorsIds))

	// Prepare the accept request
	acceptRequest := AcceptRequest{
		ProposalId: p.proposalId,
		Value:      value,
	}

	// Send AcceptRequest to all acceptors.
	for _, acceptorId := range p.acceptorsIds {
		go func(acceptorId uint64) {
			defer wg.Done()
			response, cancel := p.network.Send(p.uid, acceptorId, acceptRequest)
			defer cancel()
			select {
			case message := <-response:
				switch response := message.(type) {
				case AcceptResponse:
					if response.Accepted {
						acceptChannel <- true
					}
				default:
					// Unexpected message type.
					panic("Unexpected message type in accept stage")
				}
			case <-time.After(1 * time.Second):
				fmt.Println("Proposer[", p.uid, "timed out waiting for AcceptResponse from Acceptor[", acceptorId, "]")
			}
		}(acceptorId)
	}

	// Wait for all goroutine to complete
	wg.Wait()

	return majority <= 0
}
