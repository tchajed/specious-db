package db

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"

	"github.com/tchajed/specious-db/fs"
)

// Table interface
//
// An Table stores a entries sorted by key on disk, with an efficient
// in-memory index to find keys on disk.
//
// table format:
// entries: []Entry
// index: Index
// index_ptr: SliceHandle
//
// SliceHandle:
// offset uint64
// length uint32
//
// Note: gob encoding is somewhat complicated here (and will change to something
// more explicit); index contains several handles, each of which points to a
// gob-encoded slice, whereas actual format will encode entries in sequence and
// rely on handles and sequential decoding to identify individual entries.

type Table struct {
	name  string
	fs    fs.Filesys
	index tableIndex
}

const indexPtrOffset = 8 + 4

type SliceHandle struct {
	Offset uint64
	Length uint32
}

type tableIndex struct {
	entries []indexEntry
}

func (i tableIndex) Get(k Key) (SliceHandle, error) {
	for _, e := range i.entries {
		if e.Keys.Contains(k) {
			return e.Handle, nil
		}
	}
	return SliceHandle{}, ErrKeyMissing{}
}

type indexEntry struct {
	Handle SliceHandle
	Keys   KeyRange
}

func (i tableIndex) Keys() KeyRange {
	first := i.entries[0].Keys
	last := i.entries[len(i.entries)-1].Keys
	return KeyRange{first.Min, last.Max}
}

func readIndexData(fs fs.Filesys, name string) []byte {
	indexPtrData, err := fs.ReadAt(name, -1-indexPtrOffset, indexPtrOffset)
	if err != nil {
		panic(err)
	}
	offset := binary.LittleEndian.Uint64(indexPtrData[0:4])
	length := binary.LittleEndian.Uint32(indexPtrData[4:6])
	data, err := fs.ReadAt(name, int(offset), int(length))
	if err != nil {
		panic(err)
	}
	return data
}

func NewTable(name string, fs fs.Filesys) Table {
	indexData := readIndexData(fs, name)
	var index tableIndex
	dec := gob.NewDecoder(bytes.NewBuffer(indexData))
	dec.Decode(&index.entries)
	return Table{name, fs, index}
}

func (t Table) Get(k Key) (Value, error) {
	h, err := t.index.Get(k)
	// if handle is not found in index, then key is not present in table
	if err != nil {
		return nil, err
	}
	data, err := t.fs.ReadAt(t.name, int(h.Offset), int(h.Length))
	if err != nil {
		panic(err)
	}
	dec := gob.NewDecoder(bytes.NewBuffer(data))
	var entries []Entry
	err = dec.Decode(&entries)
	if err != nil {
		panic(err)
	}
	for _, e := range entries {
		if KeyEq(e.Key, k) {
			return e.Value, nil
		}
	}
	return nil, ErrKeyMissing{}
}

func (t Table) Keys() KeyRange {
	return t.index.Keys()
}
