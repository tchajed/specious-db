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

// TODO: standardize on fs goes last
func readIndexData(f fs.ReadFile) []byte {
	indexPtrData := f.ReadAt(f.Size()-indexPtrOffset, indexPtrOffset)
	offset := binary.LittleEndian.Uint64(indexPtrData[0:4])
	length := binary.LittleEndian.Uint32(indexPtrData[4:6])
	data := f.ReadAt(int(offset), int(length))
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

func NewTable(ident uint32, fs fs.Filesys, entries []indexEntry) Table {
	f := fs.Open(identToName(ident))
	return Table{ident, f, newTableIndex(entries)}
}

func OpenTable(ident uint32, fs fs.Filesys) Table {
	f := fs.Open(identToName(ident))
	indexData := readIndexData(f)
	var index tableIndex
	r := SliceReader{indexData}
	for r.RemainingBytes() > 0 {
		index.entries = append(index.entries, r.IndexEntry())
	}
	return Table{ident, f, index}
}

func (t Table) Get(k Key) MaybeValue {
	h := t.index.Get(k)
	// if handle is not found in index, then key is not present in table
	if !h.IsValid() {
		return NoValue
	}
	data := t.f.ReadAt(int(h.Offset), int(h.Length))
	r := SliceReader{data}
	for r.RemainingBytes() > 0 {
		e := r.KeyUpdate()
		if e.IsPut() && KeyEq(e.Key, k) {
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

func newTableWriter(f fs.File) tableWriter {
	return tableWriter{
		f:            f,
		w:            newWriter(f),
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
	w.w.KeyUpdate(e)
}

func (w *tableWriter) updateIndex(k Key) {
	if w.currentIndex == nil {
		// initialize an index entry
		w.currentIndex = &indexEntry{
			SliceHandle{Offset: w.Offset()},
			KeyRange{Min: k},
		}
	}
	w.currentIndex.Keys.Max = k
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
