package bin

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
)

func testRoundtrip(enc func(e *Encoder), dec func(d *Decoder)) {
	var b bytes.Buffer
	enc(NewEncoder(&b))
	r := NewDecoder(b.Bytes())
	dec(r)
	if r.RemainingBytes() > 0 {
		panic("decoder did not consume all bytes")
	}
}

func TestUints(t *testing.T) {
	assert := assert.New(t)
	for _, v := range []uint64{0, 3, 0x20DF135CE9DBF162, 0xfffffff} {
		testRoundtrip(func(e *Encoder) {
			e.Uint64(v)
		}, func(r *Decoder) {
			assert.Equal(v, r.Uint64(), "uint64 %v should roundtrip", v)
		})
	}

	for _, v := range []uint32{0, 3, 0xCE9DBF62, 0xffff} {
		testRoundtrip(func(e *Encoder) {
			e.Uint32(v)
		}, func(r *Decoder) {
			assert.Equal(v, r.Uint32(), "uint32 should roundtrip")
		})
	}
	for _, v := range []uint16{0, 3, 0xCE9D, 0xffff} {
		testRoundtrip(func(e *Encoder) {
			e.Uint16(v)
		}, func(r *Decoder) {
			assert.Equal(v, r.Uint16(), "uint16 should roundtrip")
		})
	}
}

func TestArray16(t *testing.T) {
	assert := assert.New(t)
	bigArray := make([]byte, 65535)
	bigArray[2] = 16
	bigArray[1023] = 12
	for i, v := range [][]byte{{1, 2, 3}, {}, bigArray} {
		testRoundtrip(func(e *Encoder) {
			e.Array16(v)
		}, func(r *Decoder) {
			assert.Equal(v, r.Array16(), "array %d should roundtrip", i)
		})
	}
}

func TestMultipleThings(t *testing.T) {
	assert := assert.New(t)
	testRoundtrip(func(e *Encoder) {
		e.Uint32(12)
		e.Uint16(7)
		e.Bytes([]byte{1, 2, 3})
	}, func(r *Decoder) {
		assert.Equal(uint32(12), r.Uint32())
		assert.Equal(uint16(7), r.Uint16())
		assert.Equal([]byte{1, 2, 3}, r.Bytes(3))
	})
}
