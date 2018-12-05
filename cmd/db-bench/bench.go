package main

import (
	"fmt"
	"math/rand"
	"time"

	"github.com/tchajed/specious-db/db"
	"github.com/tchajed/specious-db/fs"
	"github.com/tchajed/specious-db/leveldb"
)

type generator struct {
	*rand.Rand
	key uint64
}

func newGenerator() *generator {
	r := rand.New(rand.NewSource(0))
	return &generator{r, 0}
}

func (g *generator) NextKey() db.Key {
	k := g.key
	g.key++
	return k
}

func (g generator) RandomKey() db.Key {
	return g.Rand.Uint64()
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

func speciousDb() *db.Database {
	fs := fs.DirFs("benchmark.db")
	return db.Init(fs)
}

func levelDb() *leveldb.Database {
	return leveldb.New("benchmark.db")
}

type database interface {
	db.Store
	Compact()
}

type noopdb struct{}

func (d noopdb) Get(k db.Key) db.MaybeValue { return db.NoValue }
func (d noopdb) Put(k db.Key, v db.Value)   {}
func (d noopdb) Delete(k db.Key)            {}
func (d noopdb) Close()                     {}
func (d noopdb) Compact()                   {}

// TODO: add command-line arguments
func main() {
	databaseType := "specious"
	// databaseType := "leveldb"
	// databaseType := "noop"
	var db database
	switch databaseType {
	case "specious":
		db = speciousDb()
	case "leveldb":
		db = levelDb()
	case "noop":
		db = noopdb{}
	}
	g := newGenerator()
	s := newStats()
	for i := 0; i < 1000000; i++ {
		if databaseType == "specious" && i%100000 == 0 && i != 0 {
			db.Compact()
		}
		k, v := g.RandomKey(), g.Value()
		db.Put(k, v)
		s.finishOp(8 + len(v))
	}
	fmt.Println(databaseType, "database")
	s.Report()
	db.Close()
}
