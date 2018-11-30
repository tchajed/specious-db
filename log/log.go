package log

// Atomic storage for binary blobs
//
// TODO: this shouldn't be called log; tentatively let's call it a TxnWriter
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
// - Recover the committed transactions in an idempotent way.
//
// TODO: adjust this API to not use file system; should just provide a way of
// appending and committing to a file, and recovering the transactions in the
// file (higher-level processes recovered transactions and then clears them when
// it's done recovering). This will at least separate concerns better.

import (
	"encoding/gob"

	"github.com/tchajed/specious-db/fs"
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

type Log struct {
	log fs.File
	enc *gob.Encoder
}

const logFilename = "log"

func Init(fs fs.Filesys) Log {
	f := fs.Create(logFilename)
	return Log{f, gob.NewEncoder(f)}
}

func recoverTxns(fs fs.Filesys) (txns [][]byte) {
	f := fs.Open(logFilename)
	dec := gob.NewDecoder(f)
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

func Recover(fs fs.Filesys) ([][]byte, Log) {
	txns := recoverTxns(fs)
	// TODO: probably don't need to append to an existing log; however, need to
	// think about how the API works since higher-level needs a call to indicate
	// it's done recovering and log should clear the existing file and
	// re-initialize.
	f := fs.Append(logFilename)
	return txns, Log{f, gob.NewEncoder(f)}
}

func (l Log) Add(data []byte) error {
	l.enc.Encode(Record{DataRecord, data})
	l.log.Sync()
	l.enc.Encode(Record{CommitRecord, nil})
	l.log.Sync()
	return nil
}

func (l Log) Close() {
	l.log.Close()
}
