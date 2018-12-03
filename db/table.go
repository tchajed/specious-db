package db

import (
	"encoding/binary"
	"fmt"

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
	ident uint32
	f     fs.ReadFile
	index tableIndex
}

func identToName(ident uint32) string {
	return fmt.Sprintf("table-%06d.ldb", ident)
}

func (t Table) Name() string {
	return identToName(t.ident)
}

const indexPtrOffset = 8 + 4

type SliceHandle struct {
	Offset uint64
	Length uint32
}

func (h SliceHandle) IsValid() bool {
	return h.Length != 0
}

// TODO: make this an efficient data structure
type tableIndex struct {
	entries []indexEntry
}

func newTableIndex(entries []indexEntry) tableIndex {
	return tableIndex{entries}
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

func readIndexData(f fs.ReadFile) []byte {
	indexPtrData := f.ReadAt(f.Size()-indexPtrOffset, indexPtrOffset)
	offset := binary.LittleEndian.Uint64(indexPtrData[0:8])
	length := binary.LittleEndian.Uint32(indexPtrData[8 : 8+4])
	data := f.ReadAt(int(offset), int(length))
	return data
}

func NewTable(ident uint32, entries []indexEntry, fs fs.Filesys) Table {
	f := fs.Open(identToName(ident))
	return Table{ident, f, newTableIndex(entries)}
}

func OpenTable(ident uint32, fs fs.Filesys) Table {
	f := fs.Open(identToName(ident))
	indexData := readIndexData(f)
	var index tableIndex
	r := newDecoder(indexData)
	for r.RemainingBytes() > 0 {
		index.entries = append(index.entries, r.IndexEntry())
	}
	return Table{ident, f, index}
}

type MaybeKeyValue struct {
	Valid   bool
	Present bool
	Value
}

func (mu MaybeKeyValue) MaybeValue() MaybeValue {
	if !mu.Valid {
		panic("attempt to get value from an invalid value")
	}
	return MaybeValue{Present: mu.Present, Value: mu.Value}
}

func (t Table) Get(k Key) MaybeKeyValue {
	h := t.index.Get(k)
	// if handle is not found in index, then key is not present in table
	if !h.IsValid() {
		return MaybeKeyValue{Valid: false}
	}
	data := t.f.ReadAt(int(h.Offset), int(h.Length))
	r := newDecoder(data)
	for r.RemainingBytes() > 0 {
		e := r.KeyUpdate()
		if e.Key == k {
			if e.IsPut() {
				return MaybeKeyValue{Valid: true, Present: true, Value: e.Value}
			} else {
				return MaybeKeyValue{Valid: true, Present: false}
			}
		}
	}
	// key turned out to be missing
	return MaybeKeyValue{Valid: false}
}

func (t Table) Keys() KeyRange {
	return t.index.Keys()
}

type tableWriter struct {
	f            fs.File
	w            Encoder
	currentIndex *indexEntry
	entries      []indexEntry
}

func newTableWriter(f fs.File) *tableWriter {
	return &tableWriter{
		f:            f,
		w:            newEncoder(f),
		currentIndex: nil,
		entries:      nil,
	}
}

func (w tableWriter) Offset() uint64 {
	return uint64(w.w.BytesWritten())
}

func (w tableWriter) currentIndexLength() uint32 {
	return uint32(w.Offset() - w.currentIndex.Handle.Offset)
}

func (w *tableWriter) Put(e KeyUpdate) {
	start := w.Offset()
	w.w.KeyUpdate(e)
	if w.currentIndex == nil {
		// initialize an index entry
		w.currentIndex = &indexEntry{
			SliceHandle{Offset: start},
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

func (w tableWriter) Close() []indexEntry {
	w.flush()
	if len(w.entries) == 0 {
		panic("table has no values")
	}
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
	return w.entries
}
