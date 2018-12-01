package db

// The manifest tracks all the known SSTables.
//
// This includes an on-disk representation for crash safety as well as in-memory
// cache to lookup keys by first finding the right tables to search.
//
// https://play.golang.org/p/dV5lRWTnYaU

import (
	"bytes"
	"encoding/gob"
	"fmt"

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

type tableCreator struct {
	w    tableWriter
	name string
	m    *Manifest
}

func (c tableCreator) Put(e KeyUpdate) {
	c.w.Put(e)
}

func (c tableCreator) Build() Table {
	entries := c.w.Close()
	return Table{c.name, c.m.fs, newTableIndex(entries)}
}

func (m *Manifest) InstallTable(t Table) {
	// NOTE: we use the file system's atomic rename to create the manifest,
	// but could attempt to use the logging implementation
	tables := make([]string, 0, len(m.tables)+1)
	for _, old_table := range m.tables {
		tables = append(tables, old_table.name)
	}
	tables = append(tables, t.name)
	var buf []byte
	enc := gob.NewEncoder(bytes.NewBuffer(buf))
	err := enc.Encode(tables)
	if err != nil {
		panic(err)
	}
	m.fs.AtomicCreateWith("manifest", buf)

	m.tables = append(m.tables, t)
}

func (m *Manifest) NewTable(level int) tableCreator {
	id := 0 // TODO: fresh id
	name := fmt.Sprintf("table-l%d-%d.ldb", level, id)
	f := m.fs.Create(name)
	return tableCreator{newTableWriter(f), name, m}
}

// TODO: implement compaction
//
// requires iteration over entire table and writing out new table

// TODO: pick and handle files for young generation specially, coalescing duplicates between files

// TODO: streaming construction of multiple tables, splitting at some file size
