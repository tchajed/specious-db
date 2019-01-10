package fs

import (
	"fmt"
	"io/ioutil"
	"testing"

	"github.com/stretchr/testify/suite"
)

type FsSuite struct {
	suite.Suite
	fs Filesys
}

func TestFs(t *testing.T) {
	suite.Run(t, new(FsSuite))
}

func (suite *FsSuite) SetupTest() {
	suite.fs = MemFs()
}

func (suite FsSuite) CreateFile(fname string, contents []byte) {
	f := suite.fs.Create(fname)
	n, err := f.Write(contents)
	if n < len(contents) {
		panic(fmt.Errorf("short write %d/%d bytes", n, len(contents)))
	}
	if err != nil {
		panic(err)
	}
	f.Close()
}

func (suite FsSuite) ReadFile(fname string) []byte {
	f := suite.fs.Open(fname)
	data, err := ioutil.ReadAll(f)
	if err != nil {
		panic(err)
	}
	f.Close()
	return data
}

func (suite FsSuite) TestCreate() {
	suite.CreateFile("foo", []byte{2})
	suite.Equal([]byte{2}, suite.ReadFile("foo"),
		"file should have same contents as written")
}

func (suite FsSuite) TestCreateBar() {
	suite.CreateFile("bar", []byte{2})
	suite.Equal([]byte{2}, suite.ReadFile("bar"),
		"file should have same contents as written")
}

func (suite FsSuite) TestAtomicCreate() {
	suite.fs.AtomicCreateWith("foo", []byte{2})
	suite.Equal([]byte{2}, suite.ReadFile("foo"),
		"file should have correct contents")
}

func (suite FsSuite) TestTruncate() {
	suite.CreateFile("foo", []byte{1, 2, 3})
	suite.fs.Truncate("foo")
	suite.Equal([]byte{}, suite.ReadFile("foo"),
		"truncate should empty file")
}

func (suite FsSuite) TestList() {
	suite.CreateFile("foo", []byte{})
	suite.CreateFile("bar", []byte{})
	suite.Equal([]string{"/bar", "/foo"}, suite.fs.List())
}

func (suite FsSuite) TestDelete() {
	suite.CreateFile("foo", []byte{})
	suite.Equal([]string{"/foo"}, suite.fs.List())
	suite.fs.Delete("foo")
	suite.Empty(suite.fs.List())
}

func (suite FsSuite) TestRename() {
	suite.CreateFile("foo", []byte{1,2,3})
	suite.fs.Rename("foo", "bar")
	suite.Equal([]string{"/bar"}, suite.fs.List())
	suite.Equal([]byte{1,2,3}, suite.ReadFile("bar"),
		"rename should preserve contents")
}

func (suite FsSuite) TestSize() {
	suite.CreateFile("foo", []byte{1, 2, 3})
	f := suite.fs.Open("foo")
	suite.Equal(3, f.Size())
	f.Close()
}

func (suite FsSuite) TestReadAt() {
	suite.CreateFile("foo", []byte{1, 2, 3})
	f := suite.fs.Open("foo")
	suite.Equal([]byte{2}, f.ReadAt(1, 1))
	suite.Equal([]byte{2, 3}, f.ReadAt(1, 2))
}

func (suite FsSuite) TestDeleteAll() {
	suite.CreateFile("foo", nil)
	suite.CreateFile("bar", nil)
	suite.Equal(2, len(suite.fs.List()))
	DeleteAll(suite.fs)
	suite.Empty(suite.fs.List())
}
