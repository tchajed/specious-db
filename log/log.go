package log

// Atomic storage for binary blobs
//
// Supports storing binary blobs ("transactions") atomically with respect to
// crashes.
//
// API:
// - Add: commits a transaction
// - Recover: returns successfully committed transactions
// - there is no third method
//
// How to use this API:
// - Create an applications-specific Log that embeds a TxnWriter.
// - Serialize application-level operations and add then as transactions.
// - Cache all writes and expose a read API.
// - For recovery, process all updates and commit them in a crash-safe manner,
//   then clear the log file.

import (
	"bytes"
	"io"
	"io/ioutil"

	"github.com/tchajed/specious-db/bin"
)

const (
	invalidRecord uint8 = iota
	dataRecord
	commitRecord
)

// Writer gives access to a transactional log backed by an io.WriteCloser.
// Transactions are uninterpreted byte arrays. Assuming writes (appends) to this
// interface are persisted in order, log.RecoverTxns allows to recover any
// committed and persisted transactions even if the system halts in the middle
// of adding a transaction. During normal operation the caller is expected to
// record transactions in memory (probably in a non-byte-array format) and
// should not need to use the log for anything, thus the log.Writer has no read
// API.
type Writer struct {
	log io.WriteCloser
	enc *bin.Encoder
}

// New allocates a new log.Writer around a file.
func New(f io.WriteCloser) Writer {
	return Writer{f, bin.NewEncoder(f)}
}

// Add records a transaction in the log file.
func (l Writer) Add(data []byte) {
	buf := bytes.NewBuffer(make([]byte, 0, 1+2+len(data)+1))
	localEnc := bin.NewEncoder(buf)
	localEnc.Uint8(dataRecord)
	localEnc.Array16(data)
	localEnc.Uint8(commitRecord)
	l.enc.Bytes(buf.Bytes())
}

// Close finishes writing the log. The Writer should not be used afterward.
func (l Writer) Close() {
	err := l.log.Close()
	if err != nil {
		panic(err)
	}
}

// RecoverTxns returns any committed and persisted transactions from a reader
// over a log file, handling partial writes to the log.
func RecoverTxns(log io.Reader) (txns [][]byte) {
	buf, err := ioutil.ReadAll(log)
	if err != nil {
		panic(err)
	}
	dec := bin.NewDecoder(buf)
	for {
		// here we decode as much as possible, stopping early if we run out of
		// bytes (unfortunately the binary decoding library doesn't have good
		// support for stopping early and will panic if there aren't enough
		// bytes)
		if dec.RemainingBytes() == 0 {
			return
		}
		ty := dec.Uint8()
		if ty != dataRecord {
			panic("expected data record")
		}
		len := dec.Uint16()
		if dec.RemainingBytes() < int(len) {
			return
		}
		data := dec.Bytes(int(len))
		if dec.RemainingBytes() == 0 {
			return
		}
		ty = dec.Uint8()
		if ty != commitRecord {
			panic("expected commit record")
		}
		txns = append(txns, data)
	}
}
