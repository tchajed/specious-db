package bin

import (
	"encoding/binary"
	"io"
)

// Simple binary parsing/serialization library.

// Decoder streams binary data from a byte buffer.
type Decoder struct {
	buf []byte
}

// NewDecoder creates a decoder that parses data from buffer b.
//
// Retains b, which the caller should not use afterward.
func NewDecoder(b []byte) *Decoder {
	return &Decoder{b}
}

// RemainingBytes gives the number of bytes remaining in the buffer.
func (r Decoder) RemainingBytes() int {
	return len(r.buf)
}

// Bytes is a primitive decoder that reads a fixed number of bytes.
func (r *Decoder) Bytes(n int) []byte {
	d := r.buf[:n]
	r.buf = r.buf[n:]
	return d
}

// Encoder encodes values to an output stream.
type Encoder struct {
	w io.Writer
	// total bytes written since initialization
	bytesWritten int
}

// NewEncoder creates an encoder that writes data to w.
func NewEncoder(w io.Writer) *Encoder {
	return &Encoder{w: w, bytesWritten: 0}
}

// BytesWritten returns the number of bytes written to the encoder since this
// encoder was created.
func (w Encoder) BytesWritten() int {
	return w.bytesWritten
}

// Bytes is a primitive encoder that copies bytes.
func (w *Encoder) Bytes(b []byte) {
	for len(b) > 0 {
		n, err := w.w.Write(b)
		if err != nil {
			panic(err)
		}
		w.bytesWritten += n
		b = b[n:]
	}
}

// Uint64 decodes a uint64 (in little endian format).
func (r *Decoder) Uint64() uint64 {
	return binary.LittleEndian.Uint64(r.Bytes(8))
}

// Uint32 decodes a uint32 (in little endian format).
func (r *Decoder) Uint32() uint32 {
	return binary.LittleEndian.Uint32(r.Bytes(4))
}

// Uint16 decodes a uint16 (in little endian format).
func (r *Decoder) Uint16() uint16 {
	return binary.LittleEndian.Uint16(r.Bytes(2))
}

// Uint8 decodes a uint8
func (r *Decoder) Uint8() uint8 {
	return r.Bytes(1)[0]
}

// Array16 decodes an array prefixed with a 16-byte length.
func (r *Decoder) Array16() []byte {
	length := r.Uint16()
	data := r.Bytes(int(length))
	return data
}

// Uint64 encodes a uint64 (in little endian format).
func (w *Encoder) Uint64(v uint64) {
	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, v)
	w.Bytes(b)
}

// Uint32 encodes a uint32 (in little endian format).
func (w *Encoder) Uint32(v uint32) {
	b := make([]byte, 4)
	binary.LittleEndian.PutUint32(b, v)
	w.Bytes(b)
}

// Uint16 encodes a uint16 (in little endian format).
func (w *Encoder) Uint16(v uint16) {
	b := make([]byte, 2)
	binary.LittleEndian.PutUint16(b, v)
	w.Bytes(b)
}

// Uint8 encodes a uint8
func (w *Encoder) Uint8(b uint8) {
	w.Bytes([]byte{b})
}

// Array16 encodes an array prefixed with a 16-byte length.
func (w *Encoder) Array16(b []byte) {
	if len(b) >= 1<<16 {
		panic("array too large to write 16-bit length")
	}
	w.Uint16(uint16(len(b)))
	w.Bytes(b)
}
