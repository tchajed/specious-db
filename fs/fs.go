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
	List() []string
}
