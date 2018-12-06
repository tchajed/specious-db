package bin

// varint parsing, factored out since it's a complex feature to support (due to
// the need for additional bitwise operations and reasoning about those
// operations)

// VarInt parses a varint
//
// See the protocol-buffer documentation for the encoding format:
// https://developers.google.com/protocol-buffers/docs/encoding#varints.
func (r *Decoder) VarInt() uint64 {
	n := uint64(0)
	for shift := uint(0); shift < 64; shift += 7 {
		b := r.Uint8()
		n = n | (uint64(b&0x7f) << shift)
		if b&0x80 == 0 {
			return n
		}
	}
	return n
}

// VarInt encodes a 64-bit int as a varint
func (r *Encoder) VarInt(u uint64) {
	bytes := make([]byte, 0, 1)
	for {
		b := uint8(u & 0x7f)
		u = u >> 7
		if u > 0 {
			bytes = append(bytes, b|0x80)
		} else {
			// this is the most significant byte
			bytes = append(bytes, b)
			r.Bytes(bytes)
			return
		}
	}
}
