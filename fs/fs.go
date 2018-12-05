package fs

import (
	"io"
)

type File interface {
	io.WriteCloser
	Sync()
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
//   Open:      fname should exist
//   Create:    fname should not exist
//   Delete:    fname should exist
//   Truncate:  fname should exist
type Filesys interface {
	// read-only APIs

	Open(fname string) ReadFile
	List() []string
	// Debug is a convenience to list out the entire file system for debugging
	// (technically this can be implemented on top of the above API)
	Debug()

	// modifications

	Create(fname string) File
	Delete(fname string)
	Truncate(fname string)
	AtomicCreateWith(fname string, data []byte)
}
