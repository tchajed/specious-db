package db

// The manifest tracks all the known SSTables.
//
// This includes an on-disk representation for crash safety as well as in-memory
// cache to lookup keys by first finding the right tables to search.
//
// https://play.golang.org/p/dV5lRWTnYaU

import (
	"encoding/gob"

	"github.com/tchajed/specious-db/fs"
)

type Manifest struct {
	fs     fs.Filesys
	tables []Table
}

func initManifest(fs fs.Filesys) Manifest {
	f := fs.Create("manifest")
	defer f.Close()
	enc := gob.NewEncoder(f)
	err := enc.Encode(0)
	if err != nil {
		panic(err)
	}
	return Manifest{fs, nil}
}

func (m Manifest) isValidTable(name string) bool {
	for _, t := range m.tables {
		if name == t.name {
			return true
		}
	}
	return false
}

func (m Manifest) cleanup() {
	for _, f := range m.fs.List() {
		if f == "log" || f == "manifest" || m.isValidTable(f) {
			continue
		}
		m.fs.Delete(f)
	}
}

func newManifest(fs fs.Filesys) Manifest {
	f := fs.Open("manifest")
	defer f.Close()
	dec := gob.NewDecoder(f)
	var names []string
	var numTables int
	err := dec.Decode(&numTables)
	if err != nil {
		panic(err)
	}
	if numTables > 0 {
		err = dec.Decode(&names)
		if err != nil {
			panic(err)
		}
	}
	tables := make([]Table, 0, len(names))
	for _, n := range names {
		tables = append(tables, NewTable(n, fs))
	}
	m := Manifest{fs, tables}
	m.cleanup()
	return m
}

func (m *Manifest) Compact() {
	// TODO: implement compaction
	//
	// requires iteration over entire table and writing out new table
}

// TODO: pick and handle files for young generation specially, coalescing duplicates between files

// TODO: streaming construction of multiple tables, splitting at some file size
