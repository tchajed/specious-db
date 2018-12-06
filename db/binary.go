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

type Encoder struct {
	*bin.Encoder
}

func newEncoder(w io.Writer) Encoder {
	return Encoder{bin.NewEncoder(w)}
}

func (r Decoder) KeyUpdate() KeyUpdate {
	key := r.VarInt()
	length := r.Uint16()
	if length == 0xffff {
		return KeyUpdate{key, NoValue}
	}
	value := r.Bytes(int(length))
	return KeyUpdate{key, SomeValue(value)}
}

func (w *Encoder) KeyUpdate(e KeyUpdate) {
	w.VarInt(e.Key)
	if e.IsPut() {
		w.Array16(e.Value)
	} else {
		w.Uint16(0xffff)
	}
}

func (r Decoder) Handle() SliceHandle {
	offset := r.VarInt()
	length := r.VarInt()
	return SliceHandle{offset, uint32(length)}
}

func (w *Encoder) Handle(h SliceHandle) {
	w.VarInt(h.Offset)
	w.VarInt(uint64(h.Length))
}

func (r Decoder) FixedHandle() SliceHandle {
	offset := r.Uint64()
	length := r.Uint32()
	return SliceHandle{offset, length}
}

func (w *Encoder) FixedHandle(h SliceHandle) {
	w.Uint64(h.Offset)
	w.Uint32(h.Length)
}

func (r *Decoder) KeyRange() KeyRange {
	min := r.VarInt()
	max := r.VarInt()
	return KeyRange{min, max}
}

func (w *Encoder) KeyRange(keys KeyRange) {
	w.VarInt(keys.Min)
	w.VarInt(keys.Max)
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
