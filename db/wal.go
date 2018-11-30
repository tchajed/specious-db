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
	cache entrySearchTree
}

// TODO: implement search tree
type entrySearchTree struct {}

func (t entrySearchTree) Get(k Key) MaybeValue {
	panic("not implemented")
}

func (t entrySearchTree) Put(k Key, v Value) {
	panic("not implemented")
}

func (t entrySearchTree) Delete(k Key) {
	panic("not implemented")
}

func (t entrySearchTree) Stream() EntryIterator {
	// NOTE: this stream already colaesces all updates to the same key
	//
	// NOTE: streaming should be in-order, everything else relies on this
	// initial ordering
	panic("not implemented")
}

func (l dbLog) Get(k Key) MaybeValue {
	return l.cache.Get(k)
}

func (l dbLog) Put(k Key, v Value) {
	// TODO: log a put operation
	l.cache.Put(k, v)
}

func (l dbLog) Delete(k Key) {
	// TODO: log a delete operation
	l.cache.Delete(k)
}

func (l dbLog) Stream() EntryIterator {
	return l.cache.Stream()
}

func initLog(fs fs.Filesys) dbLog {
	log := log.Init(fs)
	return dbLog{log, entrySearchTree{}}
}

func recoverLog(fs fs.Filesys) dbLog {
	txns, log := log.Recover(fs)
	cache := entrySearchTree{}
	// TODO: initialize cache by processing txns
	var _ = txns
	return dbLog{log, cache}
}
