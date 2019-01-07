package fs

import (
	"fmt"
	"io"
)

// File is a writeable file.
type File interface {
	io.WriteCloser
	Sync()
}

// ReadFile is a read-only file.
type ReadFile interface {
	Size() int
	ReadAt(offset int, length int) []byte
	io.ReadCloser
}

// Stats holds some basic stats about what filesystem operations have been
// issued.
type Stats struct {
	ReadOps    int
	ReadBytes  int
	WriteOps   int
	WriteBytes int
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

	// modifications

	Create(fname string) File
	Delete(fname string)
	Truncate(fname string)
	AtomicCreateWith(fname string, data []byte)

	// performance counters
	GetStats() Stats
}

// DeleteAll deletes all files within a Filesys (which is a single directory).
func DeleteAll(fs Filesys) {
	for _, f := range fs.List() {
		fs.Delete(f)
	}
}

// Debug prints out all the files in a Filesys, along with their sizes.
func Debug(fs Filesys) {
	for _, fname := range fs.List() {
		f := fs.Open(fname)
		fmt.Printf("%-20s %3d bytes\n", fname, f.Size())
		f.Close()
	}
}
