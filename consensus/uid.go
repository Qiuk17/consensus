package consensus

import (
	"sync/atomic"
)

type UidGenerator interface {
	Next() uint64
}

type NaiveUidGenerator struct {
	next atomic.Uint64
}

func NewNaiveUidGenerator() *NaiveUidGenerator {
	generator := &NaiveUidGenerator{next: atomic.Uint64{}}
	return generator
}

func (g *NaiveUidGenerator) Next() uint64 {
	return g.next.Add(1)
}
