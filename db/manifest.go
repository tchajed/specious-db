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

// Get reads a key from the tables managed by the manifest file.
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
	fs             fs.Filesys
	ident          uint32
	w              *tableWriter
}

// CreateTable initializes a new table writer
//
// This operation requires write permissions (for silly reasons - it only
// protects the identifier counter)
func (m *Manifest) CreateTable() tableCreator {
	id := m.nextIdent
	m.nextIdent++
	f := m.fs.Create(identToName(id))
	return tableCreator{m.fs, id, newTableWriter(f)}
}

// Put adds to an in-progress background table.
//
// This operation is logically _read-only_ on the manifest's table, including
// wrt crashes.
func (c tableCreator) Put(e KeyUpdate) {
	c.w.Put(e)
}

// Close finishes writing out a background table.
//
// This operation is logically _read-only_.
func (c tableCreator) Close() Table {
	entries := c.w.Close()
	f := c.fs.Open(identToName(c.ident))
	newTable := NewTable(c.ident, f, entries)
	return newTable
}

func subsumedTables(youngTables []uint32, level1tables []uint32) map[uint32]bool {
	tablesSubsumed := make(map[uint32]bool, len(youngTables)+len(level1tables))
	for _, ident := range youngTables {
		tablesSubsumed[ident] = true
	}
	for _, ident := range level1tables {
		tablesSubsumed[ident] = true
	}
	return tablesSubsumed
}

// InstallTable adds a previously created table to the tracked tables in the manifest.
//
// Requires that the table already be stored in the right place (using
// m.CreateTable() and its associated operations).
//
// This operation requires write permissions to the manifest.
func (m *Manifest) InstallTable(newTable Table, youngTables []uint32, level1tables []uint32, level int) {
	tablesSubsumed := subsumedTables(youngTables, level1tables)
	levels := make([][]Table, 2)
	for level, tables := range m.tables {
		for _, t := range tables {
			if !tablesSubsumed[t.ident] {
				levels[level] = append(levels[level], t)
			}
		}
	}
	levels[level] = append(levels[level], newTable)
	m.tables = levels
	m.save()
	for ident := range tablesSubsumed {
		m.fs.Delete(identToName(ident))
	}

}

func (c tableCreator) CloseAndInstall(level int) {
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
