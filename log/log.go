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

type LogFile interface {
	io.WriteCloser
}

type Writer struct {
	log LogFile
	enc *bin.Encoder
}

func New(f LogFile) Writer {
	return Writer{f, bin.NewEncoder(f)}
}

func (l Writer) Add(data []byte) error {
	buf := bytes.NewBuffer(make([]byte, 0, 1+2+len(data)+1))
	localEnc := bin.NewEncoder(buf)
	localEnc.Uint8(dataRecord)
	localEnc.Array16(data)
	localEnc.Uint8(commitRecord)
	l.enc.Bytes(buf.Bytes())
	return nil
}

func (l Writer) Close() {
	l.log.Close()
}

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
