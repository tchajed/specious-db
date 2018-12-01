package fs

import (
	"bytes"
	"fmt"
	"io"
)

type memFilesys struct {
	m map[string]*memFile
}

type memFile struct {
	data []byte
}

func (f *memFile) Write(p []byte) (int, error) {
	f.data = append(f.data, p...)
	return len(p), nil
}

func (f *memFile) Close() error {
	return nil
}

func (f *memFile) Sync() error {
	return nil
}

type fileReader struct {
	io.Reader
}

func (r fileReader) Close() error { return nil }

func (f memFile) Reader() fileReader {
	data := make([]byte, len(f.data))
	copy(data, f.data)
	return fileReader{bytes.NewBuffer(data)}
}

func (fs memFilesys) get(fname string) *memFile {
	f, ok := fs.m[fname]
	if !ok {
		panic(fmt.Errorf("attempt to use non-existent file %s", fname))
	}
	return f
}

func (fs memFilesys) Open(fname string) io.ReadCloser {
	return fs.get(fname).Reader()
}

func (fs memFilesys) Create(fname string) File {
	_, ok := fs.m[fname]
	if ok {
		panic(fmt.Errorf("attempt to Create over existing file %s", fname))
	}
	f := &memFile{nil}
	fs.m[fname] = f
	return f
}

func (fs memFilesys) Append(fname string) File {
	return fs.get(fname)
}

func (fs memFilesys) ReadAt(fname string, start int, length int) []byte {
	f := fs.get(fname)
	if start >= 0 {
		return f.data[start : start+length]
	}
	startIdx := len(f.data) - (-start + 1)
	return f.data[startIdx : startIdx+length]
}

func (fs memFilesys) List() []string {
	var names []string
	for n := range fs.m {
		names = append(names, n)
	}
	return names
}

func (fs memFilesys) Delete(fname string) {
	_ = fs.get(fname)
	delete(fs.m, fname)
}

func (fs memFilesys) AtomicCreateWith(fname string, data []byte) {
	fs.m[fname] = &memFile{data}
}

// Mem creates an in-memory file system (for testing purposes)
func Mem() Filesys {
	return memFilesys{make(map[string]*memFile)}
}