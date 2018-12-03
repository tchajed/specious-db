package fs

import (
	"fmt"
	"os"

	"github.com/spf13/afero"
)

type aferoFs struct {
	fs afero.Afero
}

type readFile struct {
	afero.File
}

func (f readFile) Size() int {
	st, err := f.Stat()
	if err != nil {
		panic(err)
	}
	return int(st.Size())
}

func (f readFile) ReadAt(offset int, length int) []byte {
	p := make([]byte, length)
	n, err := f.File.ReadAt(p, int64(offset))
	if n != len(p) {
		panic(fmt.Errorf("short ReadAt of %d bytes for %s", n, f.File.Name()))
	}
	if err != nil {
		panic(err)
	}
	return p
}

func abs(fname string) string {
	return fmt.Sprintf("/%s", fname)
}

func (fs aferoFs) Open(fname string) ReadFile {
	f, err := fs.fs.Open(abs(fname))
	if err != nil {
		panic(err)
	}
	return readFile{f}
}

type writeFile struct {
	afero.File
}

func (f writeFile) Sync() {
	err := f.File.Sync()
	if err != nil {
		panic(err)
	}
}

func (fs aferoFs) Create(fname string) File {
	f, err := fs.fs.Create(abs(fname))
	if err != nil {
		panic(err)
	}
	return writeFile{f}
}

func (fs aferoFs) List() []string {
	names, err := afero.Glob(fs.fs, abs("*"))
	if err != nil {
		panic(err)
	}
	return names
}

func (fs aferoFs) Delete(fname string) {
	err := fs.fs.Remove(abs(fname))
	if err != nil {
		panic(err)
	}
}

func (fs aferoFs) Truncate(fname string) {
	f, err := fs.fs.OpenFile(abs(fname), os.O_WRONLY|os.O_TRUNC, 0)
	if err != nil {
		panic(err)
	}
	err = f.Close()
	if err != nil {
		panic(err)
	}
}

func (fs aferoFs) AtomicCreateWith(fname string, data []byte) {
	tmpFile := abs(fmt.Sprintf("%s.tmp", fname))
	err := fs.fs.WriteFile(tmpFile, data, 0x744)
	if err != nil {
		panic(err)
	}
	f, _ := fs.fs.Open(tmpFile)
	f.Sync()
	f.Close()
	err = fs.fs.Rename(tmpFile, abs(fname))
	if err != nil {
		panic(err)
	}
}

func (fs aferoFs) Debug() {
	fs.fs.Walk("/", func(path string, info os.FileInfo, err error) error {
		if err != nil {
			fmt.Printf("%-20s error %v\n", path, err)
			return err
		}
		fmt.Printf("%-20s %3d bytes\n", path, info.Size())
		return nil
	})
}

func deleteTmpFiles(fs afero.Fs) {
	tmpFiles, err := afero.Glob(fs, abs("*.tmp"))
	if err != nil {
		panic(err)
	}
	for _, n := range tmpFiles {
		err = fs.Remove(abs(n))
		if err != nil {
			panic(err)
		}
	}
}

// FromAfero creates an fs.Filesys from any Afero file system.
//
// This implementation will use absolute filenames for the database files; use
// an afero.BasePathFs to make sure all database files are created within a
// particular directory.
//
// Deletes all files named *.tmp, as a file-system recovery for AtomicCreateWith.
func FromAfero(fs afero.Fs) Filesys {
	deleteTmpFiles(fs)
	return aferoFs{afero.Afero{fs}}
}

// MemFs creates an in-memory Filesys
func MemFs() Filesys {
	fs := afero.NewMemMapFs()
	return FromAfero(fs)
}

// DirFs creates a Filesys backed by the OS, using basedir.
//
// Creates basedir if it does not exist.
func DirFs(basedir string) Filesys {
	fs := afero.NewOsFs()
	ok, err := afero.IsDir(fs, basedir)
	if err != nil {
		panic(err)
	}
	if !ok {
		err = fs.Mkdir(basedir, 0755)
		if err != nil {
			panic(err)
		}
	}
	baseFs := afero.NewBasePathFs(fs, basedir)
	return FromAfero(baseFs)
}
