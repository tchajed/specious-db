package db

import (
	"io"

	"github.com/tchajed/specious-db/bin"
)

type Decoder struct {
	*bin.Decoder
}

func newDecoder(b []byte) Decoder {
	return Decoder{bin.NewDecoder(b)}
}

func (r Decoder) Entry() Entry {
	key := r.Uint64()
	value := r.Array16()
	return Entry{key, value}
}

func (r Decoder) KeyUpdate() KeyUpdate {
	key := r.Uint64()
	length := r.Uint16()
	if length == 0xffff {
		return KeyUpdate{key, NoValue}
	}
	value := r.Bytes(int(length))
	return KeyUpdate{key, SomeValue(value)}
}

func (r Decoder) Handle() SliceHandle {
	offset := r.Uint64()
	length := r.Uint32()
	return SliceHandle{offset, length}
}

func (r *Decoder) KeyRange() KeyRange {
	min := r.Uint64()
	max := r.Uint64()
	return KeyRange{min, max}
}

type Encoder struct {
	*bin.Encoder
}

func newEncoder(w io.Writer) Encoder {
	return Encoder{bin.NewEncoder(w)}
}

func (w *Encoder) Entry(e Entry) {
	w.Uint64(e.Key)
	w.Array16(e.Value)
}

func (w *Encoder) KeyUpdate(e KeyUpdate) {
	w.Uint64(e.Key)
	if e.IsPut() {
		w.Array16(e.Value)
	} else {
		w.Uint16(0xffff)
	}
}

func (w *Encoder) Handle(h SliceHandle) {
	w.Uint64(h.Offset)
	w.Uint32(h.Length)
}

func (w *Encoder) KeyRange(keys KeyRange) {
	w.Uint64(keys.Min)
	w.Uint64(keys.Max)
}

func (r *Decoder) IndexEntry() indexEntry {
	h := r.Handle()
	keys := r.KeyRange()
	return indexEntry{h, keys}
}

func (w *Encoder) IndexEntry(e indexEntry) {
	w.Handle(e.Handle)
	w.KeyRange(e.Keys)
}
