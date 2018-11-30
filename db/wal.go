package db

// Key-value store logging structure
//
// Provides a mini key-value store on top of a write-ahead log that only serves
// Gets from the log (missing keys may have been migrated to SSTables)

import (
	"github.com/tchajed/specious-db/fs"
	"github.com/tchajed/specious-db/log"
)

// higher-level interface to log that supports writing operations and reading
// from a cache of the log
type dbLog struct {
	log log.Log
	// TODO: in-memory cache (skip list for efficient search)
}

func (l dbLog) Get(k Key) MaybeValue {
	// TODO: read from in-memory cache
	panic("not implemented")
}

func (l dbLog) Put(k Key, v Value) {
	// TODO: log a put operation
	// TODO: add (k, v) to in-memory cache
	panic("not implemented")
}

func (l dbLog) Delete(k Key) {
	// TODO: log a delete operation
	// TODO: delete k from cache
	panic("not implemented")
}

func initLog(fs fs.Filesys) dbLog {
	log := log.Init(fs)
	return dbLog{log}
}

func recoverLog(fs fs.Filesys) dbLog {
	txns, log := log.Recover(fs)
	// TODO: initialize cache by processing txns
	var _ = txns
	return dbLog{log}
}
