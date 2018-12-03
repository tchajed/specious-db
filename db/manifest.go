package db

// The manifest tracks all the known SSTables.
//
// This includes an on-disk representation for crash safety as well as in-memory
// cache to lookup keys by first finding the right tables to search.
//
// on-disk representation:
// numTables uint32
// idents [numTables]uint32
// logTruncated uint8 (boolean)

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
	e.Uint8(1)
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
			return mu.MaybeValue()
		}
	}
	return NoValue
}

func newManifest(fs fs.Filesys) (mf Manifest, logTruncated bool) {
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
	logTruncated = dec.Uint8() == 1
	if dec.RemainingBytes() > 0 {
		panic(fmt.Errorf("manifest has %d leftover bytes", dec.RemainingBytes()))
	}

	tables := make([]Table, 0, len(idents))
	for _, i := range idents {
		tables = append(tables, OpenTable(i, fs))
	}
	m := Manifest{fs, tables}
	m.cleanup()
	return m, logTruncated
}

type tableCreator struct {
	w     *tableWriter
	ident uint32
	m     *Manifest
}

func (c tableCreator) Put(e KeyUpdate) {
	c.w.Put(e)
}

func (c tableCreator) Build() Table {
	entries := c.w.Close()
	return NewTable(c.ident, entries, c.m.fs)
}

func (m *Manifest) InstallTable(t Table) {
	// NOTE: we use the file system's atomic rename to create the manifest,
	// but could attempt to use the logging implementation
	var buf bytes.Buffer
	enc := newEncoder(&buf)
	enc.Uint32(uint32(len(m.tables)) + 1)
	for _, t0 := range m.tables {
		enc.Uint32(t0.ident)
	}
	enc.Uint32(t.ident)
	enc.Uint8(0)
	m.fs.AtomicCreateWith("manifest", buf.Bytes())
	m.tables = append(m.tables, t)
}

func (m *Manifest) MarkLogTruncated() {
	var buf bytes.Buffer
	enc := newEncoder(&buf)
	enc.Uint32(uint32(len(m.tables)))
	for _, t := range m.tables {
		enc.Uint32(t.ident)
	}
	enc.Uint8(1)
	m.fs.AtomicCreateWith("manifest", buf.Bytes())
}

// TODO: might make sure sense for manifest to just create a table from an
// iterator over updates (that function can subsume tableCreator and maybe still
// use a local tableWriter)
func (m *Manifest) NewTable() tableCreator {
	// NOTE: need to guarantee that table IDs increase and we know about all the
	// tables for this name to be fresh
	id := uint32(1)
	if len(m.tables) > 0 {
		id = m.tables[len(m.tables)-1].ident + 1
	}
	f := m.fs.Create(identToName(id))
	return tableCreator{newTableWriter(f), id, m}
}

// TODO: implement compaction
//
// requires iteration over entire table and writing out new table

// TODO: pick and handle files for young generation specially, coalescing duplicates between files

// TODO: streaming construction of multiple tables, splitting at some file size
