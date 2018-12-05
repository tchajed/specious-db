package main

import (
	"fmt"

	"github.com/tchajed/specious-db/db"
	"github.com/tchajed/specious-db/fs"
	"github.com/tchajed/specious-db/leveldb"
)

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
	s := NewBench()
	for i := 0; i < 1000000; i++ {
		if databaseType == "specious" && i%100000 == 0 && i != 0 {
			db.Compact()
		}
		k, v := s.RandomKey(), s.Value()
		db.Put(k, v)
		s.FinishedSingleOp(8 + len(v))
	}
	s.Done()
	fmt.Println(databaseType, "database")
	s.Report()
	db.Close()
}
