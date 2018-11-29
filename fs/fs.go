package fs

import "io"

// note that this interface only exposes a single directory
type Filesys interface {
	Open(fname string) (io.ReadCloser, error)
	Create(fname string) (io.WriteCloser, error)
	List() []string
}
