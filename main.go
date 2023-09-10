package main

import (
	c "consensus/consensus"
	"consensus/raft"
	"consensus/statemachine"
	"fmt"
	"time"
)

func main() {
	network := c.NewReliableNetwork()
	consensus := raft.NewRaft(10, network)
	// consensus := paxos.NewPaxos(10, 500, network)
	sm := statemachine.SingleStateMachine{
		Consensus: consensus,
	}

	go func() {
		sm.Run()
	}()

	// Wait for consensus to be ready.
	<-time.After(1 * time.Second)
	// Propose some values at the same time.
	sm.RacePropose(1, 2, 3, 4, 5)
	fmt.Println("Propose Done")
	// Wait for consensus to be reached.
	// For raft, the result is that all nodes commit the values in the same order.
	// For paxos, the result is that all nodes commit one of the values.
	<-time.After(2 * time.Second)
}
