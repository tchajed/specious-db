package main

import (
	"fmt"
	"math/rand"
	"time"

	"github.com/tchajed/specious-db/db"
	"github.com/tchajed/specious-db/fs"
)

type generator struct {
	*rand.Rand
}

func newGenerator() generator {
	return generator{rand.New(rand.NewSource(0))}
}

func (g generator) Key() db.Key {
	return g.Uint64()
}

func (g generator) Value() db.Value {
	b := make([]byte, 100)
	g.Read(b)
	return b
}

type stats struct {
	Ops   int
	Bytes int
	Start time.Time
}

func newStats() stats {
	return stats{Ops: 0, Bytes: 0, Start: time.Now()}
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

func main() {
	fs := fs.DirFs("benchmark.db")
	db := db.Init(fs)
	g := newGenerator()
	s := newStats()
	for i := 0; i < 1000000; i++ {
		if i%100000 == 0 && i != 0 {
			db.Compact()
		}
		db.Put(g.Key(), g.Value())
		s.finishOp(8 + 100)
	}
	s.Report()
	db.Close()
}
