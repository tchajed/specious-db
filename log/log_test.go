package log

import "testing"

import "github.com/stretchr/testify/assert"
import "github.com/spf13/afero"

func TestLogEmpty(t *testing.T) {
	assert := assert.New(t)
	fs := afero.NewMemMapFs()
	f, _ := fs.Create("log")
	f.Close()
	txns := RecoverTxns(f)
	assert.Empty(txns, "empty file should be an empty log")
}

func newLog() (afero.Fs, Writer) {
	fs := afero.NewMemMapFs()
	f, _ := fs.Create("log")
	return fs, New(f)
}

func recoverLog(fs afero.Fs) [][]byte {
	f, _ := fs.Open("log")
	return RecoverTxns(f)
}

func TestLogNoTxns(t *testing.T) {
	assert := assert.New(t)
	fs, w := newLog()
	w.Close()
	txns := recoverLog(fs)
	assert.Empty(txns, "log should have no transactions")
}

func TestLogSingle(t *testing.T) {
	assert := assert.New(t)
	fs, w := newLog()
	w.Add([]byte{1, 2, 3})
	w.Close()
	txns := recoverLog(fs)
	assert.Equal([][]byte{
		{1, 2, 3},
	}, txns, "should recover single txn")
}

func TestLogMultiple(t *testing.T) {
	assert := assert.New(t)
	fs, w := newLog()
	w.Add([]byte{1, 2, 3})
	w.Add([]byte{4})
	w.Close()
	txns := recoverLog(fs)
	assert.Equal([][]byte{
		{1, 2, 3},
		{4},
	}, txns, "should recover multiple txns")
}

func TestLogEmptyTxn(t *testing.T) {
	assert := assert.New(t)
	fs, w := newLog()
	w.Add([]byte{1})
	w.Add([]byte{})
	w.Add([]byte{4})
	w.Close()
	txns := recoverLog(fs)
	assert.Equal([][]byte{
		{1},
		// note that due to gob, this is nil instead of an empty byte slice
		// (though these are functionally identical in Go for the most part)
		nil,
		{4},
	}, txns, "should recover an empty txn")
}
