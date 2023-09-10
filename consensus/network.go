package consensus

import (
	"context"
	"fmt"
)

type Endpoint interface {
	// Receive should return a channel *immediately* to simulate a connection.
	// When `ctx` is done, the connection should be closed.
	Receive(message any, ctx context.Context) (<-chan any, error)
}

type Network interface {
	// Send `message` to destination.
	// The first return value is a channel holding responses.
	// The second return value is a cancel function to close the connection.
	Send(from uint64, to uint64, message any) (<-chan any, context.CancelFunc)
	Register(uid uint64, endpoint Endpoint)
}

func Send[ResponseT any](network Network, from uint64, to uint64, message any) (<-chan ResponseT, context.CancelFunc) {
	originalChannel, cancel := network.Send(from, to, message)
	outputChannel := make(chan ResponseT)
	go func() {
		defer close(outputChannel)
		for response := range originalChannel {
			switch typedResponse := response.(type) {
			case ResponseT:
				outputChannel <- typedResponse
			default:
				panic("type mismatch")
			}
		}
	}()
	return outputChannel, cancel
}

type ReliableNetwork struct {
	endpoints map[uint64]Endpoint
}

func NewReliableNetwork() *ReliableNetwork {
	return &ReliableNetwork{endpoints: make(map[uint64]Endpoint)}
}

func (n *ReliableNetwork) Send(from uint64, to uint64, message any) (<-chan any, context.CancelFunc) {
	destination := n.endpoints[to]
	ctx, cancel := context.WithCancel(context.Background())
	channel, err := destination.Receive(message, ctx)
	if err != nil {
		fmt.Println(err)
		channel = nil
	}
	return channel, cancel
}

func (n *ReliableNetwork) Register(uid uint64, endpoint Endpoint) {
	n.endpoints[uid] = endpoint
}
