package main

import (
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"runtime"
	"runtime/pprof"
	"strings"
	"time"

	"github.com/tchajed/specious-db/db"
	"github.com/tchajed/specious-db/db/memdb"
	"github.com/tchajed/specious-db/fs"
	"github.com/tchajed/specious-db/leveldb"
)

const dbPath = "benchmark.db"

type database interface {
	db.Store
	Compact()
}

func initFs() fs.Filesys {
	switch *fsType {
	case "dir":
		filesys := fs.DirFs(dbPath)
		fs.DeleteAll(filesys)
		return filesys
	case "mem":
		return fs.MemFs()
	}
	return nil
}

func initDb(filesys fs.Filesys) database {
	switch *dbType {
	case "specious":
		fs.DeleteAll(filesys)
		return db.Init(filesys)
	case "specious-mem":
		return db.Init(filesys)
	case "leveldb":
		os.RemoveAll(dbPath)
		return leveldb.New(dbPath)
	case "mem":
		return memdb.New()
	}
	panic(fmt.Errorf("unknown database type %s", *dbType))
}

func showNum(i int) string {
	if i > 2000 {
		if i%1000 == 0 {
			return fmt.Sprintf("%dK", i/1000)
		}
		return fmt.Sprintf("%.1fK", float64(i)/1000)
	}
	return fmt.Sprintf("%d", i)
}

var benchmarks = flag.String("benchmarks", "fillseq,readseq,init,fillrandom,readrandom", "comma-separated list of benchmarks to run")
var dbType = flag.String("db", "specious", "database to use (specious|leveldb|mem)")
var fsType = flag.String("fs", "dir", "filesystem to use for specious-db (dir|mem)")
var numEntries = flag.Int("entries", 1000000, "number of entries to put in database")
var numReads = flag.Int("reads", -1, "number of reads to perform (-1 to copy entries)")
var finalCompact = flag.Bool("final-compact", false, "force a compaction at end of benchmark")
var deleteDatabase = flag.Bool("delete-db", false, "delete database directory on completion")
var cpuprofile = flag.String("cpuprofile", "", "write cpu profile to `file`")
var memprofile = flag.String("memprofile", "", "write memory cpu profile to `file`")
var printStats = flag.Bool("stats", false, "print out filesystem stats")

func writeMemProfile(fname string) {
	f, err := os.Create(fname)
	if err != nil {
		log.Fatal("could not create memory profile: ", err)
	}
	runtime.GC() // get up-to-date statistics
	if err := pprof.WriteHeapProfile(f); err != nil {
		log.Fatal("could not write memory profile: ", err)
	}
	f.Close()
}

func writeFile(sz int, data []byte, s *stats, fs fs.Filesys) {
	f := fs.Create("benchfile")
	for sz > 0 {
		buf := data
		if sz < len(data) {
			buf = buf[:sz]
		}
		n, err := f.Write(buf)
		if err != nil {
			panic(err)
		}
		s.FinishedSingleOp(n)
		sz -= n
	}
	f.Close()
}

func readFile(chunkSize int, s *stats, fs fs.Filesys) {
	buf := make([]byte, chunkSize)
	f := fs.Open("benchfile")
	defer func() {
		f.Close()
		fs.Delete("benchfile")
	}()
	for {
		n, err := f.Read(buf)
		s.FinishedSingleOp(n)
		if err == io.EOF {
			return
		}
		if err != nil {
			panic(err)
		}
	}
}

func runBenchmarks(fs fs.Filesys, db database) time.Time {
	if *cpuprofile != "" {
		f, err := os.Create(*cpuprofile)
		if err != nil {
			log.Fatal("could not create CPU profile: ", err)
		}
		if err := pprof.StartCPUProfile(f); err != nil {
			log.Fatal("could not start CPU profile: ", err)
		}
		defer pprof.StopCPUProfile()
	}
	if *memprofile != "" {
		defer writeMemProfile(*memprofile)
	}

	benchmarkNames := strings.Split(*benchmarks, ",")
	for _, name := range benchmarkNames {
		s := NewBench(name)
		switch name {
		case "fillseq":
			for i := 0; i < *numEntries; i++ {
				k, v := s.NextKey(), s.Value()
				db.Put(k, v)
				s.FinishedSingleOp(8 + len(v))
			}
			if *finalCompact {
				db.Compact()
			}
		case "fillrandom":
			for i := 0; i < *numEntries; i++ {
				k, v := s.RandomKey(*numEntries), s.Value()
				db.Put(k, v)
				s.FinishedSingleOp(8 + len(v))
			}
			if *finalCompact {
				db.Compact()
			}
		case "readseq":
			for i := 0; i < *numReads; i++ {
				v := db.Get(s.NextKey())
				if v.Present {
					s.FinishedSingleOp(8 + len(v.Value))
				}
			}
		case "readrandom":
			// read in a different random order from random writes
			s.ReSeed(1)
			for i := 0; i < *numReads; i++ {
				v := db.Get(s.RandomKey(*numEntries))
				if v.Present {
					s.FinishedSingleOp(8 + len(v.Value))
				}
			}
		case "init":
			db.Close()
			db = initDb(fs)
			s.FinishedSingleOp(0)
		case "fs-read":
			// does not use database
			readFile(4096, s.stats, fs)
		case "fs-write":
			// does not use database
			data := make([]byte, 4096)
			s.Read(data)
			writeFile(16*1024*1024, data, s.stats, fs)
		default:
			fmt.Fprintf(os.Stderr, "unknown benchmark %s\n", name)
			os.Exit(1)
		}
		s.Report()
	}
	end := time.Now()
	db.Close()
	return end
}

func main() {
	flag.Parse()

	if len(flag.Args()) > 0 {
		fmt.Fprintln(os.Stderr, "extra command line arguments", flag.Args())
		flag.Usage()
		os.Exit(1)
	}

	if *numReads == -1 {
		*numReads = *numEntries
	}

	totalBytes := float64(*numEntries * (8 + 100))
	reportedDatabase := *dbType
	if *fsType != "dir" {
		reportedDatabase += fmt.Sprintf(" (%s)", *fsType)
	}
	for _, info := range []struct {
		Key   string
		Value string
	}{
		{"database", reportedDatabase},
		{"entries", showNum(*numEntries)},
		{"final compaction?", fmt.Sprintf("%v", *finalCompact)},
		{"total data (MB)", fmt.Sprintf("%.1f", totalBytes/(1024*1024))},
	} {
		fmt.Printf("%20s %s\n", info.Key+":", info.Value)
	}
	fmt.Println(strings.Repeat("-", 30))

	fs := initFs()
	db := initDb(fs)
	start := time.Now()
	end := runBenchmarks(fs, db)

	if *printStats {
		fsstats := fs.GetStats()
		writes := stats{fsstats.WriteOps, fsstats.WriteBytes, start, &end}
		reads := stats{fsstats.ReadOps, fsstats.ReadBytes, start, &end}
		fmt.Printf("%-20s : %s [%6d kops]\n", "[meta] fs-writes", writes.formatStats(), writes.Ops/1000)
		fmt.Printf("%-20s : %s [%6d kops]\n", "[meta] fs-reads", reads.formatStats(), reads.Ops/1000)
	}

	if *deleteDatabase {
		os.RemoveAll(dbPath)
	}
}
