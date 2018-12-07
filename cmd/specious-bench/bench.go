package main

import (
	"fmt"
	"math/rand"
	"time"

	"github.com/tchajed/specious-db/db"
)

type generator struct {
	*rand.Rand
	key uint64
}

func newGenerator() *generator {
	r := rand.New(rand.NewSource(0))
	return &generator{r, 0}
}

func (g generator) ReSeed(i int64) {
	g.Rand.Seed(i)
}

func (g *generator) NextKey() db.Key {
	k := g.key
	g.key++
	return db.Key(k)
}

func (g generator) RandomKey(max int) db.Key {
	n := g.Rand.Int63n(int64(max))
	return db.Key(uint64(n))
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

// done marks the benchmark finished.
//
// Records a final timestamp in a stats object.
func (s *stats) done() {
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

func (s stats) formatStats() string {
	if s.Bytes == 0 {
		if s.Ops == 1 {
			return fmt.Sprintf("%7.3f micros", s.MicrosPerOp())
		}
		return fmt.Sprintf("%7.3f micros/op", s.MicrosPerOp())
	}
	return fmt.Sprintf("%7.3f micros/op; %6.1f MB/s",
		s.MicrosPerOp(),
		s.MegabytesPerSec())
}

// BenchState tracks information for a single benchmark.
type BenchState struct {
	name string
	*generator
	*stats
}

// NewBench initializes a BenchState.
func NewBench(name string) BenchState {
	return BenchState{name, newGenerator(), newStats()}
}

// Report finishes the benchmark and prints final statistics.
func (s BenchState) Report() {
	s.stats.done()
	fmt.Printf("%-20s : %s\n", s.name, s.stats.formatStats())
}
