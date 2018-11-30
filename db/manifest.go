package db

import (
	"encoding/gob"

	"github.com/tchajed/specious-db/fs"
)

type Manifest struct {
	fs     fs.Filesys
	tables []Table
}

func initManifest(fs fs.Filesys) Manifest {
	f, err := fs.Create("manifest")
	if err != nil {
		panic(err)
	}
	defer f.Close()
	enc := gob.NewEncoder(f)
	enc.Encode(0)
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
	f, err := fs.Open("manifest")
	if err != nil {
		panic(err)
	}
	defer f.Close()
	dec := gob.NewDecoder(f)
	var names []string
	var numTables int
	err = dec.Decode(&numTables)
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
}
