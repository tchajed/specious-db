package fs

import (
	"io"
)

type File interface {
	io.WriteCloser
	Sync() error
}

// note that this interface only exposes a single directory
type Filesys interface {
	Open(fname string) (io.ReadCloser, error)
	Create(fname string) (File, error)
	// like create but append to an existing file
	Append(fname string) (File, error)
	// read a fixed part of a file
	// TODO: end up calling this many times when we could probably cache the
	// open file
	ReadAt(fname string, start int, length int) ([]byte, error)
	List() []string
	Delete(fname string) error
	AtomicCreateWith(fname string, data []byte) error
}
