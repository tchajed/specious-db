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
	End   *time.Time
}

func newStats() *stats {
	return &stats{Ops: 0, Bytes: 0, Start: time.Now()}
}

// FinishedSingleOp records finishing an operation that processed some number of
// bytes.
func (s *stats) FinishedSingleOp(bytes int) {
	s.Ops++
	s.Bytes += bytes
}

// Done marks the benchmark finished.
//
// Records a final timestamp in a stats object.
func (s *stats) Done() {
	if s.End != nil {
		panic("stats object marked done multiple times")
	}
	t := time.Now()
	s.End = &t
}

func (s stats) seconds() float64 {
	return s.End.Sub(s.Start).Seconds()
}

func (s stats) MicrosPerOp() float64 {
	return (s.seconds() * 1e6) / float64(s.Ops)
}

func (s stats) MegabytesPerSec() float64 {
	mb := float64(s.Bytes) / (1024 * 1024)
	return mb / s.seconds()
}

func (s stats) Report() {
	fmt.Printf("%7.3f micros/op; %6.1f MB/s\n",
		s.MicrosPerOp(),
		s.MegabytesPerSec())
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
