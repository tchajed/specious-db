package main

import (
	"flag"
	"fmt"
	"os"
	"strings"

	"github.com/tchajed/specious-db/db"
	"github.com/tchajed/specious-db/fs"
	"github.com/tchajed/specious-db/leveldb"
)

const dbPath = "benchmark.db"

func speciousDb() *db.Database {
	os.RemoveAll(dbPath)
	fs := fs.DirFs(dbPath)
	return db.Init(fs)
}

func levelDb() *leveldb.Database {
	os.RemoveAll(dbPath)
	return leveldb.New(dbPath)
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

func showNum(i int) string {
	if i > 2000 {
		if i%1000 == 0 {
			return fmt.Sprintf("%dK", i/1000)
		}
		return fmt.Sprintf("%.1fK", float64(i)/1000)
	}
	return fmt.Sprintf("%d", i)
}

func main() {
	dbType := flag.String("db", "specious", "database to use (specious|leveldb|noop)")
	numEntries := flag.Int("entries", 1000000, "number of entries to put in database")
	var compactEvery int
	flag.IntVar(&compactEvery, "compact-every", 50000, "compact database after x entries")
	finalCompact := flag.Bool("final-compact", false, "force a compaction at end of benchmark")
	deleteDatabase := flag.Bool("delete-db", false, "delete database directory on completion")
	flag.Parse()

	if len(flag.Args()) > 0 {
		fmt.Fprintln(os.Stderr, "extra command line arguments", flag.Args())
		flag.Usage()
		os.Exit(1)
	}

	var db database
	switch *dbType {
	case "specious":
		db = speciousDb()
	case "leveldb":
		db = levelDb()
	case "noop":
		db = noopdb{}
	default:
		fmt.Fprintln(os.Stderr, "unknown database")
		os.Exit(1)
	}

	totalBytes := float64(*numEntries * (8 + 100))
	for _, info := range []struct {
		Key   string
		Value string
	}{
		{"database", *dbType},
		{"entries", showNum(*numEntries)},
		{"compaction every", showNum(compactEvery)},
		{"final compaction?", fmt.Sprintf("%v", *finalCompact)},
		{"total data (MB)", fmt.Sprintf("%.1f", totalBytes/(1024*1024))},
	} {
		fmt.Printf("%20s %s\n", info.Key+":", info.Value)
	}
	fmt.Println(strings.Repeat("-", 30))

	s := NewBench()
	for i := 0; i < *numEntries; i++ {
		if compactEvery > 0 && i%compactEvery == 0 && i != 0 {
			db.Compact()
		}
		k, v := s.RandomKey(), s.Value()
		db.Put(k, v)
		s.FinishedSingleOp(8 + len(v))
	}
	if *finalCompact {
		db.Compact()
	}
	s.Done()

	db.Close()
	s.Report()
	if *deleteDatabase {
		os.RemoveAll(dbPath)
	}
}
