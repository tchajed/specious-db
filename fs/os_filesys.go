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

type file struct{ *os.File }

func (f file) Size() int {
	st, err := f.Stat()
	if err != nil {
		panic(err)
	}
	return int(st.Size())
}

func (fs osFilesys) Open(fname string) ReadFile {
	f, err := os.Open(fs.path(fname))
	if err != nil {
		panic(err)
	}
	return file{f}
}

func (fs osFilesys) Create(fname string) File {
	f, err := os.Create(fs.path(fname))
	if err != nil {
		panic(err)
	}
	return f
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
