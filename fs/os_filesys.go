package fs

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path"
)

type osFilesys struct {
	basedir string
}

func (fs osFilesys) path(name string) string {
	return path.Join(fs.basedir, name)
}

func (fs osFilesys) Open(fname string) io.ReadCloser {
	f, err := os.Open(fs.path(fname))
	if err != nil {
		panic(err)
	}
	return f
}

func (fs osFilesys) Create(fname string) File {
	f, err := os.Create(fs.path(fname))
	if err != nil {
		panic(err)
	}
	return f
}

func (fs osFilesys) Append(fname string) File {
	f, err := os.OpenFile(fs.path(fname), os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		panic(err)
	}
	return f
}

func (fs osFilesys) ReadAt(fname string, start int, length int) []byte {
	panic("not implemented")
}

func (fs osFilesys) List() []string {
	files, err := ioutil.ReadDir(fs.basedir)
	if err != nil {
		panic(err)
	}
	var names []string
	for _, info := range files {
		names = append(names, info.Name())
	}
	return names
}

func (fs osFilesys) Delete(fname string) {
	err := os.Remove(fs.path(fname))
	if err != nil {
		panic(err)
	}
}

func (fs osFilesys) AtomicCreateWith(fname string, data []byte) {
	f, err := ioutil.TempFile(os.TempDir(), fmt.Sprintf("%s-*.tmp", fname))
	if err != nil {
		panic(err)
	}
	_, err = io.Copy(f, bytes.NewBuffer(data))
	if err != nil {
		_ = f.Close()
		panic(err)
	}
	_ = f.Sync()
	_ = f.Close()
	err = os.Rename(f.Name(), fs.path(fname))
	if err != nil {
		panic(err)
	}
}

// FromBasedir creates a filesystem backed by the operating system, hosted in a directory.
//
// Creates the base directory if it does not exist.
func FromBasedir(basedir string) Filesys {
	_, err := os.Stat(basedir)
	if os.IsExist(err) {
		err = os.Mkdir(basedir, 0755)
	}
	if err != nil {
		panic(err)
	}
	return osFilesys{basedir}
}
