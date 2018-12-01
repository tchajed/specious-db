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
//   TODO: truncating the log file should be a safe way to clear it

import (
	"encoding/gob"
	"io"
)

type recordType uint8

const (
	InvalidRecord recordType = iota
	DataRecord
	CommitRecord
)

type Record struct {
	Type recordType
	Data []byte
}

type LogFile interface {
	io.WriteCloser
	Sync() error
}

type Writer struct {
	log LogFile
	enc *gob.Encoder
}

func New(f LogFile) Writer {
	return Writer{f, gob.NewEncoder(f)}
}

func (l Writer) Add(data []byte) error {
	l.enc.Encode(Record{DataRecord, data})
	l.log.Sync()
	l.enc.Encode(Record{CommitRecord, nil})
	l.log.Sync()
	return nil
}

func (l Writer) Close() {
	l.log.Close()
}

func RecoverTxns(log io.Reader) (txns [][]byte) {
	dec := gob.NewDecoder(log)
	for {
		var data Record
		err := dec.Decode(&data)
		if err != nil {
			// interpret this as a partial transaction
			return
		}
		if data.Type != DataRecord {
			panic("expected data record")
		}
		var commit Record
		err = dec.Decode(&commit)
		if err != nil {
			// data record was not successfully committed, so ignore it
			return
		}
		if commit.Type != CommitRecord {
			panic("expected commit record")
		}
		txns = append(txns, data.Data)
	}
}
