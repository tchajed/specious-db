package log

import (
	"encoding/gob"
	"errors"
	"io"

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

type LogWriter struct {
	log fs.File
	enc *gob.Encoder
}

const logFilename = "log"

func NewWriter(fs fs.Filesys) LogWriter {
	f, err := fs.Create(logFilename)
	if err != nil {
		panic(err)
	}
	return LogWriter{f, gob.NewEncoder(f)}
}

func (l LogWriter) Add(data []byte) error {
	l.enc.Encode(Record{DataRecord, data})
	l.log.Sync()
	l.enc.Encode(Record{CommitRecord, nil})
	l.log.Sync()
	return nil
}

func (l LogWriter) Close() {
	l.log.Close()
}

type LogReader struct {
	log io.ReadCloser
	dec *gob.Decoder
}

func NewReader(fs fs.Filesys) LogReader {
	f, err := fs.Open(logFilename)
	if err != nil {
		panic(err)
	}
	return LogReader{f, gob.NewDecoder(f)}
}

func (l LogReader) Next() ([]byte, error) {
	var data Record
	err := l.dec.Decode(&data)
	if err != nil {
		// interpret this as a partial transaction and ignore it
		return nil, nil
	}
	if data.Type != DataRecord {
		return nil, errors.New("expected data record next")
	}
	var commit Record
	err = l.dec.Decode(&commit)
	if err != nil {
		// data record was not successfully committed, so ignore it
		return nil, nil
	}
	if commit.Type != CommitRecord {
		return nil, errors.New("expected commit record")
	}
	return data.Data, nil
}
