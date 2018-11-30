package db

import (
	"encoding/binary"

	"github.com/tchajed/specious-db/fs"
)

// Table interface
//
// An Table stores a entries sorted by key on disk, with an efficient
// in-memory index to find keys on disk.
//
// table format:
// entries: []Entry
// index: []indexEntry
// index_ptr: SliceHandle
//
// Entry:
// key uint64
// valueLen uint16
// valueData [valueLen]byte
//
// SliceHandle:
// offset uint64
// length uint32
//
// The entries and index are not length-prefixed, since SliceHandles delimit
// what ranges need to be parsed.

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

func (h SliceHandle) IsValid() bool {
	return h.Length != 0
}

type tableIndex struct {
	entries []indexEntry
}

func (i tableIndex) Get(k Key) SliceHandle {
	for _, e := range i.entries {
		if e.Keys.Contains(k) {
			return e.Handle
		}
	}
	return SliceHandle{}
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
	indexPtrData := fs.ReadAt(name, -1-indexPtrOffset, indexPtrOffset)
	offset := binary.LittleEndian.Uint64(indexPtrData[0:4])
	length := binary.LittleEndian.Uint32(indexPtrData[4:6])
	data := fs.ReadAt(name, int(offset), int(length))
	return data
}

func (r *SliceReader) IndexEntry() indexEntry {
	h := r.Handle()
	keys := r.KeyRange()
	return indexEntry{h, keys}
}

func (w *BinaryWriter) IndexEntry(e indexEntry) {
	w.Handle(e.Handle)
	w.KeyRange(e.Keys)
}

func NewTable(name string, fs fs.Filesys) Table {
	indexData := readIndexData(fs, name)
	var index tableIndex
	r := SliceReader{indexData}
	for r.RemainingBytes() > 0 {
		index.entries = append(index.entries, r.IndexEntry())
	}
	return Table{name, fs, index}
}

func (t Table) Get(k Key) MaybeValue {
	h := t.index.Get(k)
	// if handle is not found in index, then key is not present in table
	if !h.IsValid() {
		return NoValue
	}
	data := t.fs.ReadAt(t.name, int(h.Offset), int(h.Length))
	r := SliceReader{data}
	for r.RemainingBytes() > 0 {
		e := r.Entry()
		if KeyEq(e.Key, k) {
			return SomeValue(e.Value)
		}
	}
	return NoValue
}

func (t Table) Keys() KeyRange {
	return t.index.Keys()
}

type tableWriter struct {
	f            fs.File
	w            BinaryWriter
	currentIndex *indexEntry
	entries      []indexEntry
}

func (w tableWriter) Offset() uint64 {
	return uint64(w.w.BytesWritten())
}

func (w tableWriter) currentIndexLength() uint32 {
	return uint32(w.Offset() - w.currentIndex.Handle.Offset)
}

func (w *tableWriter) Put(e Entry) {
	w.w.Entry(e)
	if w.currentIndex == nil {
		// initialize an index entry
		w.currentIndex = &indexEntry{
			SliceHandle{Offset: w.Offset()},
			KeyRange{Min: e.Key},
		}
	}
	w.currentIndex.Keys.Max = e.Key
	// periodic flush to create some index entries
	if w.currentIndexLength() > 100 {
		w.flush()
	}
}

// flush the current index entry
func (w *tableWriter) flush() {
	if w.currentIndex != nil {
		w.currentIndex.Handle.Length = w.currentIndexLength()
		w.entries = append(w.entries, *w.currentIndex)
		w.currentIndex = nil
	}
}

func (w tableWriter) Close() {
	w.flush()
	indexStart := w.Offset()
	for _, e := range w.entries {
		w.w.IndexEntry(e)
	}
	indexHandle := SliceHandle{indexStart, uint32(w.Offset() - indexStart)}
	w.w.Handle(indexHandle)
	err := w.f.Close()
	if err != nil {
		panic(err)
	}
}
