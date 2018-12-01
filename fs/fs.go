package fs

import (
	"io"
)

type File interface {
	io.WriteCloser
	Sync() error
}

type ReadFile interface {
	Size() int
	ReadAt(offset int, length int) []byte
	io.ReadCloser
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
	Open(fname string) ReadFile
	Create(fname string) File
	List() []string
	Delete(fname string)
	AtomicCreateWith(fname string, data []byte)
}
