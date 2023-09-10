package paxos

import (
	"consensus/consensus"
	"fmt"
	"math/rand"
	"time"
)

type Paxos struct {
	ProposerCount uint64
	AcceptorCount uint64

	proposers    map[uint64]*Proposer
	acceptors    map[uint64]*Acceptor
	proposersIds []uint64
	acceptorsIds []uint64

	network consensus.Network

	// The channel to receive chosen events.
	chosenChannel chan ChosenProposal
}

func NewPaxos(proposerCount uint64, acceptorCount uint64, network consensus.Network) *Paxos {
	uidGenerator := consensus.NewNaiveUidGenerator()
	chosenChannel := make(chan ChosenProposal)

	proposers := make(map[uint64]*Proposer)
	acceptors := make(map[uint64]*Acceptor)
	proposersIds := make([]uint64, 0)
	acceptorsIds := make([]uint64, 0)

	// Create all acceptors.
	for i := uint64(0); i < acceptorCount; i++ {
		nextId := uidGenerator.Next()
		acceptorsIds = append(acceptorsIds, nextId)
		newAcceptor := &Acceptor{} // TODO: ADD THIS
		acceptors[nextId] = newAcceptor
		network.Register(nextId, newAcceptor)
	}

	// Create all proposers.
	for i := uint64(0); i < proposerCount; i++ {
		nextId := uidGenerator.Next()
		proposersIds = append(proposersIds, nextId)
		newProposer := NewProposer(nextId, acceptorsIds, network, consensus.NewNaiveUidGenerator(), chosenChannel)
		proposers[nextId] = newProposer
		network.Register(nextId, newProposer)
	}

	return &Paxos{
		ProposerCount: proposerCount,
		AcceptorCount: acceptorCount,
		proposers:     proposers,
		acceptors:     acceptors,
		proposersIds:  proposersIds,
		acceptorsIds:  acceptorsIds,
		network:       network,
		chosenChannel: chosenChannel,
	}
}

func (p *Paxos) Reset() {
	for _, acceptor := range p.acceptors {
		acceptor.init()
	}
}

func (p *Paxos) Propose(value uint64) error {
	// Randomly choose a proposer
	var proposerId uint64
	for {
		proposerId = p.proposersIds[rand.Uint64()%p.ProposerCount]
		if !p.proposers[proposerId].IsLocked {
			break
		}
	}
	resultChannel, cancel := p.network.Send(0, proposerId, ProposeNewValueRequest{NewValue: value})
	defer cancel()
	switch response := (<-resultChannel).(type) {
	case ProposeNewValueResponse:
		switch response.Result {
		case PrepareFailed:
			fmt.Println("prepare stage failed, now retry (value: ", value, ")")
			<-time.After(time.Duration(rand.Intn(50)) * time.Millisecond)
			return p.Propose(value)
		case AcceptFailed:
			fmt.Println("accept stage failed, now retry (value: ", value, ")")
			<-time.After(time.Duration(rand.Intn(50)) * time.Millisecond)
			return p.Propose(value)
		default:
			return nil
		}
	default:
		panic("error response type")
	}
}

func (p *Paxos) Commit() <-chan uint64 {
	commitChannel := make(chan uint64)
	go func() {
		defer close(commitChannel)
		for chosenProposal := range p.chosenChannel {
			fmt.Println("Paxos: <id: ", chosenProposal.ProposalId, ", v: ", chosenProposal.Value, "> is chosen.")
			commitChannel <- chosenProposal.Value
		}
	}()
	return commitChannel
}
