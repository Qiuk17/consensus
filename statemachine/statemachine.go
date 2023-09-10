package statemachine

import (
	"consensus/consensus"
	"fmt"
	"sync"
)

type SingleStateMachine struct {
	Consensus consensus.ConsensusProvider
	state     uint64
}

func (sm *SingleStateMachine) Propose(value uint64) {
	sm.Consensus.Reset()
	go func() {
		if err := sm.Consensus.Propose(value); err != nil {
			fmt.Printf("failed to propose value: %v", err)
		}
	}()
}

func (sm *SingleStateMachine) RacePropose(values ...uint64) {
	sm.Consensus.Reset()
	wg := sync.WaitGroup{}
	barrier := make(chan struct{})
	for _, value := range values {
		wg.Add(1)
		go func(newValue uint64) {
			<-barrier
			if err := sm.Consensus.Propose(newValue); err != nil {
				fmt.Printf("failed to propose value[%d]: %v", newValue, err)
			}
			wg.Done()
		}(value)
	}
	close(barrier)
	wg.Wait()
}

func (sm *SingleStateMachine) Run() {
	for value := range sm.Consensus.Commit() {
		sm.state = value
		fmt.Println("StateMachine gets committed value: ", value)
	}
}
