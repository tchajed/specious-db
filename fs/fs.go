package fs

import (
	"io"
)

type File interface {
	io.WriteCloser
	Sync() error
}

// Filesys is a database-specific API for accessing the file system.
//
// Note that an instance of this interface only exposes a single directory
// (there are no directory names in these methods).
//
// Callers are expected to follow some rules when calling this API:
// - Open: fname should exist
// - Create: fname should not exist
// - ReadAt: fname should exist and start and length should be in-bounds (start
//   can be negative, in which case it's interpreted with respect to the end of the
//   file; this should not wrap to the beginning of the file).
// - Delete: fname should exist
type Filesys interface {
	Open(fname string) io.ReadCloser
	Create(fname string) File
	// like create but append to an existing file
	Append(fname string) File
	// read a fixed part of a file
	// TODO: end up calling this many times when we could probably cache the
	// open file
	ReadAt(fname string, start int, length int) []byte
	List() []string
	Delete(fname string)
	// will need at some point for updating manifest
	// AtomicCreateWith(fname string, data []byte)
}
