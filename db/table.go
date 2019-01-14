package db

import (
	"bufio"
	"fmt"

	"github.com/tchajed/specious-db/fs"
)

// Table interface
//
// An Table stores updates (entries for puts and deleted keys) sorted by key on
// disk, with an efficient in-memory index to find keys on disk.
//
// table format:
// entries: KeyUpdate*
// index: IndexEntry*
// index_ptr: FixedHandle
//
// We use FixedHandle rather than Handle for the final index ptr so that it can
// be read with a fixed-offset read.
//
// The entries and index are not length-prefixed, since SliceHandles delimit
// what ranges need to be parsed.

// A Table is a handle to and index over a table, the basic immutable storage
// unit of the database (the equivalent of an SSTable in LevelDB, which is the
// SSTable of Bigtable).
type Table struct {
	ident uint32
	f     fs.ReadFile
	index tableIndex
}

func identToName(ident uint32) string {
	return fmt.Sprintf("table-%06d.ldb", ident)
}

// Name returns the filename used to store this table.
func (t Table) Name() string {
	return identToName(t.ident)
}

const indexPtrOffset = 8 + 4

// A SliceHandle represents a slice into a file.
//
// This is what LevelDB calls a BlockHandle, but we don't have the same general
// notion of a variable-sized block.
type SliceHandle struct {
	Offset uint64
	Length uint32
}

// IsValid reports whether a SliceHandle is valid, which requires that it
// addresses a non-empty range.
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
	if !entries[mid].Keys.Contains(k) {
		panic("logical error in binsearch")
	}
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
	h := newDecoder(indexPtrData).FixedHandle()
	data := f.ReadAt(int(h.Offset), int(h.Length))
	return data
}

// NewTable creates the in-memory structure representing a table
func NewTable(ident uint32, f fs.ReadFile, entries []indexEntry) Table {
	return Table{ident, f, newTableIndex(entries)}
}

// OpenTable reads a table on-disk, initializing the in-memory cache.
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

// MaybeMaybeValue is a poor man's option (option Value).
type MaybeMaybeValue struct {
	Valid bool
	MaybeValue
}

func (t Table) readIndexEntry(h SliceHandle) Decoder {
	data := t.f.ReadAt(int(h.Offset), int(h.Length))
	return newDecoder(data)
}

// Get reads a key from the table.
//
// Since tables represent only part of the database, this Get returns a
// MaybeMaybeValue to represent a key that is not part of the table, as opposed
// to a key the table has a deletion marker for.
func (t Table) Get(k Key) MaybeMaybeValue {
	h := t.index.Get(k)
	// if handle is not found in index, then key is not present in table
	if !h.IsValid() {
		return MaybeMaybeValue{Valid: false}
	}
	r := t.readIndexEntry(h)
	for r.RemainingBytes() > 0 {
		e := r.KeyUpdate()
		if e.Key == k {
			if e.IsPut() {
				return MaybeMaybeValue{true, MaybeValue{Present: true, Value: e.Value}}
			}
			return MaybeMaybeValue{true, MaybeValue{Present: false}}
		}
	}
	// key turned out to be missing
	return MaybeMaybeValue{Valid: false}
}

type tableIterator struct {
	t       Table
	updates []KeyUpdate
	// index of next entry to read for more updates
	nextEntry int
}

func newIterator(t Table) *tableIterator {
	return &tableIterator{t: t, updates: nil, nextEntry: 0}
}

// fill re-fills the upcoming updates, if possible.
//
// Requires that i.updates is empty (that is, there actually are no buffered
// updates).
func (i *tableIterator) fill() {
	if len(i.updates) != 0 {
		panic("fill should only be called when no updates are buffered")
	}
	if i.nextEntry < len(i.t.index.entries) {
		r := i.t.readIndexEntry(i.t.index.entries[i.nextEntry].Handle)
		i.nextEntry++
		for r.RemainingBytes() > 0 {
			i.updates = append(i.updates, r.KeyUpdate())
		}
	}
	// could not fill, actually out of updates
}

func (i *tableIterator) HasNext() bool {
	if len(i.updates) > 0 {
		return true
	}
	i.fill()
	return len(i.updates) > 0
}

func (i *tableIterator) Next() KeyUpdate {
	// HasNext has returned true, so there are filled and buffered updates.
	u := i.updates[0]
	i.updates = i.updates[1:]
	return u
}

// Updates returns all the updates (puts an deletes) the table holds.
func (t Table) Updates() UpdateIterator {
	return newIterator(t)
}

// Keys gives the range of keys covered by this table.
func (t Table) Keys() KeyRange {
	return t.index.Keys()
}

type bufFile struct {
	f fs.File
	*bufio.Writer
}

func (f bufFile) Close() {
	err := f.Writer.Flush()
	if err != nil {
		panic(err)
	}
	err = f.f.Close()
	if err != nil {
		panic(err)
	}
}

func newBufferedFile(f fs.File, size int) bufFile {
	buf := bufio.NewWriterSize(f, size)
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
	w.w.FixedHandle(indexHandle)
	w.f.Close()
	return w.entries
}
