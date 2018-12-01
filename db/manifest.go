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

func (m Manifest) isKnownTable(name string) bool {
	for _, t := range m.tables {
		if name == t.Name() {
			return true
		}
	}
	return false
}

func (m Manifest) cleanup() {
	for _, f := range m.fs.List() {
		if f == "log" || f == "manifest" || m.isKnownTable(f) {
			continue
		}
		m.fs.Delete(f)
	}
}

func (m Manifest) Get(k Key) MaybeValue {
	// NOTE: need to traverse in reverse _chronological_ order so later updates overwrite earlier ones
	// TODO: add a search index over table ranges to efficiently find table
	for i := len(m.tables) - 1; i >= 0; i-- {
		mv := m.tables[i].Get(k)
		if mv.Present {
			return mv
		}
	}
	return NoValue
}

func newManifest(fs fs.Filesys) Manifest {
	f := fs.Open("manifest")
	defer f.Close()
	dec := gob.NewDecoder(f)
	var idents []uint32
	var numTables int
	err := dec.Decode(&numTables)
	if err != nil {
		panic(err)
	}
	if numTables > 0 {
		err = dec.Decode(&idents)
		if err != nil {
			panic(err)
		}
	}
	tables := make([]Table, 0, len(idents))
	for _, i := range idents {
		tables = append(tables, NewTable(i, fs))
	}
	m := Manifest{fs, tables}
	m.cleanup()
	return m
}

type tableCreator struct {
	w     tableWriter
	ident uint32
	m     *Manifest
}

func (c tableCreator) Put(e KeyUpdate) {
	c.w.Put(e)
}

func (c tableCreator) Build() Table {
	entries := c.w.Close()
	return Table{c.ident, c.m.fs, newTableIndex(entries)}
}

func (m *Manifest) InstallTable(t Table) {
	// NOTE: we use the file system's atomic rename to create the manifest,
	// but could attempt to use the logging implementation
	tables := make([]uint32, 0, len(m.tables)+1)
	for _, old_table := range m.tables {
		tables = append(tables, old_table.ident)
	}
	tables = append(tables, t.ident)
	var buf []byte
	enc := gob.NewEncoder(bytes.NewBuffer(buf))
	err := enc.Encode(tables)
	if err != nil {
		panic(err)
	}
	m.fs.AtomicCreateWith("manifest", buf)

	m.tables = append(m.tables, t)
}

func (m *Manifest) NewTable() tableCreator {
	// NOTE: need to guarantee that table IDs increase and we know about all the
	// tables for this name to be fresh
	id := m.tables[len(m.tables)-1].ident + 1
	f := m.fs.Create(identToName(id))
	return tableCreator{newTableWriter(f), id, m}
}

// TODO: implement compaction
//
// requires iteration over entire table and writing out new table

// TODO: pick and handle files for young generation specially, coalescing duplicates between files

// TODO: streaming construction of multiple tables, splitting at some file size
