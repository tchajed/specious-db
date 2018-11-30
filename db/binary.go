package db

import (
	"encoding/binary"
	"io"
)

// Simple binary parsing/serialization library.

type SliceReader struct {
	buf []byte
}

func (r SliceReader) RemainingBytes() int {
	return len(r.buf)
}

func (r *SliceReader) Bytes(n int) []byte {
	d := r.buf[:n]
	r.buf = r.buf[n:]
	return d
}

func (r *SliceReader) Uint64() uint64 {
	return binary.LittleEndian.Uint64(r.Bytes(8))
}

func (r *SliceReader) Uint32() uint32 {
	return binary.LittleEndian.Uint32(r.Bytes(4))
}

func (r *SliceReader) Uint16() uint16 {
	return binary.LittleEndian.Uint16(r.Bytes(2))
}

func (r *SliceReader) Array16() []byte {
	length := r.Uint16()
	data := r.Bytes(int(length))
	return data
}

func (r *SliceReader) Entry() Entry {
	key := r.Uint64()
	value := r.Array16()
	return Entry{key, value}
}

func (r *SliceReader) Handle() SliceHandle {
	offset := r.Uint64()
	length := r.Uint32()
	return SliceHandle{offset, length}
}

func (r *SliceReader) KeyRange() KeyRange {
	min := r.Uint64()
	max := r.Uint64()
	return KeyRange{min, max}
}

type BinaryWriter struct {
	w io.Writer
	// total bytes written since initialization
	bytesWritten int
}

func newWriter(w io.Writer) BinaryWriter {
	return BinaryWriter{w: w, bytesWritten: 0}
}

func (w BinaryWriter) BytesWritten() int {
	return w.bytesWritten
}

func (w *BinaryWriter) write(b []byte) {
	for len(b) > 0 {
		n, err := w.w.Write(b)
		if err != nil {
			panic(err)
		}
		w.bytesWritten += n
		b = b[n:]
	}
}

func (w *BinaryWriter) Uint64(v uint64) {
	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, v)
	w.write(b)
}

func (w *BinaryWriter) Uint32(v uint32) {
	b := make([]byte, 4)
	binary.LittleEndian.PutUint32(b, v)
	w.write(b)
}

func (w *BinaryWriter) Uint16(v uint16) {
	b := make([]byte, 2)
	binary.LittleEndian.PutUint16(b, v)
	w.write(b)
}

func (w *BinaryWriter) Array16(b []byte) {
	if len(b) >= 1<<16 {
		panic("array too large to write 16-bit length")
	}
	w.Uint16(uint16(len(b)))
	w.write(b)
}

func (w *BinaryWriter) Entry(e Entry) {
	w.Uint64(e.Key)
	w.Array16(e.Value)
}

func (w *BinaryWriter) Handle(h SliceHandle) {
	w.Uint64(h.Offset)
	w.Uint32(h.Length)
}

func (w *BinaryWriter) KeyRange(keys KeyRange) {
	w.Uint64(keys.Min)
	w.Uint64(keys.Max)
}
