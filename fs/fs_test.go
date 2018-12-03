package fs

import (
	"io/ioutil"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCreate(t *testing.T) {
	assert := assert.New(t)
	fs := MemFs()
	{
		f := fs.Create("foo")
		f.Write([]byte{2})
		f.Close()
	}
	{
		f := fs.Open("foo")
		data, _ := ioutil.ReadAll(f)
		assert.Equal([]byte{2}, data,
			"file should have same contents as written")
	}
}

func TestAtomicCreate(t *testing.T) {
	assert := assert.New(t)
	fs := MemFs()
	fs.AtomicCreateWith("foo", []byte{2})
	f := fs.Open("foo")
	data, _ := ioutil.ReadAll(f)
	assert.Equal([]byte{2}, data,
		"file should have correct contents")
}
