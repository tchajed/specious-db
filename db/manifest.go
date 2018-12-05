package db

// The manifest tracks all the known SSTables.
//
// This includes an on-disk representation for crash safety as well as in-memory
// cache to lookup keys by first finding the right tables to search.
//
// on-disk representation:
// numTables uint32
// idents [numTables]uint32

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"path"

	"github.com/tchajed/specious-db/fs"
)

type Manifest struct {
	fs     fs.Filesys
	tables []Table
}

func initManifest(fs fs.Filesys) Manifest {
	f := fs.Create("manifest")
	defer f.Close()
	e := newEncoder(f)
	e.Uint32(0)
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
		f = path.Base(f)
		if f == "log" || f == "manifest" || m.isKnownTable(f) {
			continue
		}
		fmt.Println("deleting obsolete file", f)
		m.fs.Delete(f)
	}
}

func (m Manifest) Get(k Key) MaybeValue {
	// NOTE: need to traverse in reverse _chronological_ order so later updates overwrite earlier ones
	// TODO: add a search index over table ranges to efficiently find table
	for i := len(m.tables) - 1; i >= 0; i-- {
		mu := m.tables[i].Get(k)
		if mu.Valid {
			return mu.MaybeValue
		}
	}
	return NoValue
}

func newManifest(fs fs.Filesys) Manifest {
	f := fs.Open("manifest")
	data, err := ioutil.ReadAll(f)
	if err != nil {
		panic(err)
	}
	f.Close()
	dec := newDecoder(data)
	numIdents := dec.Uint32()
	idents := make([]uint32, 0, numIdents)
	for i := 0; i < int(numIdents); i++ {
		idents = append(idents, dec.Uint32())
	}
	if dec.RemainingBytes() > 0 {
		panic(fmt.Errorf("manifest has %d leftover bytes", dec.RemainingBytes()))
	}

	tables := make([]Table, 0, len(idents))
	for _, i := range idents {
		tables = append(tables, OpenTable(i, fs))
	}
	m := Manifest{fs, tables}
	m.cleanup()
	return m
}

type tableCreator struct {
	// think of the tableCreator as being a set of methods on a manifest, keyed
	// by a (new, uninstalled) table ident
	m     *Manifest
	ident uint32
	w     *tableWriter
}

func (c tableCreator) Put(e KeyUpdate) {
	c.w.Put(e)
}

func (c tableCreator) CloseAndInstall() {
	entries := c.w.Close()
	f := c.m.fs.Open(identToName(c.ident))
	t := NewTable(c.ident, f, entries)
	c.m.installTable(t)
}

func (m *Manifest) installTable(t Table) {
	// NOTE: we use the file system's atomic rename to create the manifest,
	// but could attempt to use the logging implementation
	var buf bytes.Buffer
	enc := newEncoder(&buf)
	enc.Uint32(uint32(len(m.tables)) + 1)
	for _, t0 := range m.tables {
		enc.Uint32(t0.ident)
	}
	enc.Uint32(t.ident)
	m.fs.AtomicCreateWith("manifest", buf.Bytes())
	m.tables = append(m.tables, t)
}

func (m *Manifest) CreateTable() tableCreator {
	// NOTE: need to guarantee that table IDs increase and we know about all the
	// tables for this name to be fresh
	id := uint32(1)
	if len(m.tables) > 0 {
		id = m.tables[len(m.tables)-1].ident + 1
	}
	f := m.fs.Create(identToName(id))
	return tableCreator{m, id, newTableWriter(f)}
}

// TODO: implement compaction from L0 to L1
//
// needs to treat L0 specially; must take all files from a prefix so that the
// latest update is always reflected

// TODO: implement streaming construction of multiple tables, splitting at some
// file size
