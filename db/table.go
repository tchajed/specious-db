package db

import (
	"bufio"
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

type tableIndex struct {
	// entries are for sorted, disjoint ranges of keys
	entries []indexEntry
}

func newTableIndex(entries []indexEntry) tableIndex {
	return tableIndex{entries}
}

// binSearch returns the index of the entry in entries that contains k, or
// -1 if k is not present
func binSearch(entries []indexEntry, k Key) int {
	if len(entries) == 0 {
		return -1
	}
	mid := len(entries) / 2
	if k < entries[mid].Keys.Min {
		lowerHalf := entries[:mid]
		return binSearch(lowerHalf, k)
	} else if k > entries[mid].Keys.Max {
		upperHalf := entries[mid+1:]
		upperHalfIndex := binSearch(upperHalf, k)
		if upperHalfIndex == -1 {
			return -1
		}
		return upperHalfIndex + mid + 1
	}
	// mid.Keys.Contains(k)
	return mid
}

func (i tableIndex) Get(k Key) SliceHandle {
	index := binSearch(i.entries, k)
	if index == -1 {
		return SliceHandle{}
	}
	return i.entries[index].Handle
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

// NewTable creates the in-memory structure representing a table
func NewTable(ident uint32, f fs.ReadFile, entries []indexEntry) Table {
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

type MaybeMaybeValue struct {
	Valid bool
	MaybeValue
}

func (t Table) Get(k Key) MaybeMaybeValue {
	h := t.index.Get(k)
	// if handle is not found in index, then key is not present in table
	if !h.IsValid() {
		return MaybeMaybeValue{Valid: false}
	}
	data := t.f.ReadAt(int(h.Offset), int(h.Length))
	r := newDecoder(data)
	for r.RemainingBytes() > 0 {
		e := r.KeyUpdate()
		if e.Key == k {
			if e.IsPut() {
				return MaybeMaybeValue{true, MaybeValue{Present: true, Value: e.Value}}
			} else {
				return MaybeMaybeValue{true, MaybeValue{Present: false}}
			}
		}
	}
	// key turned out to be missing
	return MaybeMaybeValue{Valid: false}
}

func (t Table) Keys() KeyRange {
	return t.index.Keys()
}

type bufFile struct {
	f fs.File
	*bufio.Writer
}

func (f bufFile) Close() {
	f.Writer.Flush()
	err := f.f.Close()
	if err != nil {
		panic(err)
	}
}

func newBufferedFile(f fs.File, size int) bufFile {
	buf := bufio.NewWriterSize(f, 4*1024)
	return bufFile{f, buf}
}

type tableWriter struct {
	f            bufFile
	w            Encoder
	currentIndex *indexEntry
	currentKeys  int
	// cache of entries written, to initialize the in-memory table upon
	// finishing
	entries []indexEntry
}

func newTableWriter(f fs.File) *tableWriter {
	bw := newBufferedFile(f, 4*1024*1024)
	return &tableWriter{
		f: bw,
		w: newEncoder(bw),
	}
}

func (w tableWriter) offset() uint64 {
	return uint64(w.w.BytesWritten())
}

// Put adds an update to an in-progress table.
//
// Requires that updates be ordered by key.
func (w *tableWriter) Put(e KeyUpdate) {
	start := w.offset()
	w.w.KeyUpdate(e)
	if w.currentIndex == nil {
		// initialize an index entry
		w.currentIndex = &indexEntry{
			SliceHandle{Offset: start},
			KeyRange{Min: e.Key},
		}
	}
	if e.Key < w.currentIndex.Keys.Max {
		panic("out-of-order updates to table")
	}
	w.currentIndex.Keys.Max = e.Key
	w.currentKeys++
	// periodic flush to create some index entries
	if w.currentKeys >= 10 {
		w.flush()
	}
}

// flush the current index entry
func (w *tableWriter) flush() {
	if w.currentIndex != nil {
		length := w.offset() - w.currentIndex.Handle.Offset
		w.currentIndex.Handle.Length = uint32(length)
		w.entries = append(w.entries, *w.currentIndex)
		w.currentIndex = nil
		w.currentKeys = 0
	}
}

func (w tableWriter) Close() []indexEntry {
	w.flush()
	if len(w.entries) == 0 {
		panic("table has no values")
	}
	indexStart := w.offset()
	for _, e := range w.entries {
		w.w.IndexEntry(e)
	}
	indexHandle := SliceHandle{indexStart, uint32(w.offset() - indexStart)}
	w.w.Handle(indexHandle)
	w.f.Close()
	return w.entries
}
