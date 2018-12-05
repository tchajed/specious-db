package main

import (
	"fmt"
	"math/rand"
	"time"
)

type generator struct {
	*rand.Rand
	key uint64
}

func newGenerator() *generator {
	r := rand.New(rand.NewSource(0))
	return &generator{r, 0}
}

func (g *generator) NextKey() uint64 {
	k := g.key
	g.key++
	return k
}

func (g generator) RandomKey() uint64 {
	return g.Rand.Uint64()
}

func (g generator) Value() []byte {
	b := make([]byte, 100)
	g.Read(b)
	return b
}

type stats struct {
	Ops   int
	Bytes int
	Start time.Time
}

func newStats() *stats {
	return &stats{Ops: 0, Bytes: 0, Start: time.Now()}
}

func (s *stats) finishOp(bytes int) {
	s.Ops++
	s.Bytes += bytes
}

func (s stats) Report() {
	micros := time.Since(s.Start).Seconds() * 1e6
	fmt.Printf("%6.3f micros/op; %6.1f MB/s\n",
		micros/float64(s.Ops),
		float64(s.Bytes)/(1024*1024)/(micros/1e6))
}

// BenchState tracks information for a single benchmark.
type BenchState struct {
	*generator
	*stats
}

// NewBench initializes a BenchState.
func NewBench() BenchState {
	return BenchState{newGenerator(), newStats()}
}
