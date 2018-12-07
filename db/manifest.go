package db

// The manifest tracks all the known SSTables.
//
// This includes an on-disk representation for crash safety as well as in-memory
// cache to lookup keys by first finding the right tables to search.
//
// on-disk representation:
//   numTables uint32
//   tables [numTables]tableInfo
//
// tableInfo:
//   level uint8
//   ident uint32
//
// We only support two levels, so the level is always either 0 or 1.

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"path"

	"github.com/tchajed/specious-db/fs"
)

// A Manifest is a handle to a set of Tables, along with a separate on-disk data
// structure to track which tables are part of the database. It manages the
// underlying tables, recovering and writing to them as necessary, and exports
// an interface to create tables and install them into the manifest in a
// crash-safe manner.
type Manifest struct {
	fs        fs.Filesys
	tables    [][]Table
	nextIdent uint32
}

func initManifest(fs fs.Filesys) Manifest {
	f := fs.Create("manifest")
	defer f.Close()
	e := newEncoder(f)
	e.Uint32(0)
	return Manifest{fs, make([][]Table, 2), 1}
}

func (m Manifest) isKnownTable(name string) bool {
	for _, tables := range m.tables {
		for _, t := range tables {
			if name == t.Name() {
				return true
			}
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
	// NOTE: need to traverse in reverse _chronological_ order so later updates
	// overwrite earlier ones
	//
	// TODO: add a search index over table ranges to efficiently find table
	for _, tables := range m.tables {
		for i := len(tables) - 1; i >= 0; i-- {
			if !tables[i].Keys().Contains(k) {
				continue
			}
			mu := tables[i].Get(k)
			if mu.Valid {
				return mu.MaybeValue
			}
		}
	}
	return NoValue
}

func recoverManifest(fs fs.Filesys) Manifest {
	f := fs.Open("manifest")
	data, err := ioutil.ReadAll(f)
	if err != nil {
		panic(err)
	}
	f.Close()
	dec := newDecoder(data)
	numTables := dec.Uint32()
	tables := make([][]Table, 2)
	maxIdent := uint32(1)
	for i := 0; i < int(numTables); i++ {
		if dec.RemainingBytes() == 0 {
			panic(fmt.Errorf("manifest with %d entries is cut off", numTables))
		}
		level := dec.Uint8()
		if int(level) >= len(tables) {
			panic(fmt.Errorf("invalid level %d", level))
		}
		ident := dec.Uint32()
		if ident > maxIdent {
			maxIdent = ident
		}
		tables[level] = append(tables[level], OpenTable(ident, fs))
	}
	if dec.RemainingBytes() > 0 {
		panic(fmt.Errorf("manifest has %d leftover bytes", dec.RemainingBytes()))
	}

	m := Manifest{fs, tables, maxIdent + 1}
	m.cleanup()
	return m
}

type tableCreator struct {
	// think of the tableCreator as being a set of methods on a manifest, keyed
	// by a (new, uninstalled) table ident
	m              *Manifest
	ident          uint32
	w              *tableWriter
	tablesSubsumed map[uint32]bool
}

func (m *Manifest) CreateTable(youngTables []uint32, level1tables []uint32) tableCreator {
	id := m.nextIdent
	m.nextIdent++
	f := m.fs.Create(identToName(id))
	tablesSubsumed := make(map[uint32]bool, len(youngTables)+len(level1tables))
	for _, ident := range youngTables {
		tablesSubsumed[ident] = true
	}
	for _, ident := range level1tables {
		tablesSubsumed[ident] = true
	}
	return tableCreator{m, id, newTableWriter(f), tablesSubsumed}
}

func (c tableCreator) Put(e KeyUpdate) {
	c.w.Put(e)
}

func (c tableCreator) CloseAndInstall(level int) {
	entries := c.w.Close()
	f := c.m.fs.Open(identToName(c.ident))
	newTable := NewTable(c.ident, f, entries)
	levels := make([][]Table, 2)
	for level, tables := range c.m.tables {
		for _, t := range tables {
			if !c.tablesSubsumed[t.ident] {
				levels[level] = append(levels[level], t)
			}
		}
	}
	levels[level] = append(levels[level], newTable)
	c.m.tables = levels
	c.m.save()
	for ident := range c.tablesSubsumed {
		c.m.fs.Delete(identToName(ident))
	}
}

// Save writes out a representation of the manifest to disk (atomically).
func (m *Manifest) save() {
	var buf bytes.Buffer
	enc := newEncoder(&buf)
	numTables := 0
	for _, tables := range m.tables {
		numTables += len(tables)
	}
	enc.Uint32(uint32(numTables))
	for level, tables := range m.tables {
		for _, t := range tables {
			enc.Uint8(uint8(level))
			enc.Uint32(t.ident)
		}
	}
	// NOTE: we use the file system's atomic rename to create the manifest, but
	// could attempt to use the logging implementation
	m.fs.AtomicCreateWith("manifest", buf.Bytes())
}

// TODO: implement streaming construction of multiple tables, splitting at some
// file size
