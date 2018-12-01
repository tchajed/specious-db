package db

// Key-value store logging structure
//
// Provides a mini key-value store on top of a write-ahead log that only serves
// Gets from the log (missing keys may have been migrated to SSTables)

import (
	"sort"

	"github.com/tchajed/specious-db/fs"
	"github.com/tchajed/specious-db/log"
)

// higher-level interface to log that supports writing operations and reading
// from a cache of the log
type dbLog struct {
	log   log.Log
	cache entrySearchTree
}

type entrySearchTree struct {
	// works because keys are uint64s
	cache map[Key]MaybeValue
}

func (t entrySearchTree) Get(k Key) MaybeValue {
	v, ok := t.cache[k]
	if ok {
		return v
	} else {
		return NoValue
	}
}

func (t entrySearchTree) Put(k Key, v Value) {
	t.cache[k] = SomeValue(v)
}

func (t entrySearchTree) Delete(k Key) {
	t.cache[k] = NoValue
}

type KeyUpdate struct {
	Key
	MaybeValue
}

func (u KeyUpdate) IsPut() bool {
	return u.MaybeValue.Present
}

func (t entrySearchTree) Updates() []KeyUpdate {
	updates := make([]KeyUpdate, 0, len(t.cache))
	for k, ku := range t.cache {
		updates = append(updates, KeyUpdate{k, ku})
	}
	sort.Slice(updates, func(i, j int) bool { return updates[i].Key < updates[j].Key })
	return updates
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

func (l dbLog) Updates() []KeyUpdate {
	return l.cache.Updates()
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

func (l dbLog) Close() {
	l.log.Close()
}
