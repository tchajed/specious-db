package db

// Key-value store logging structure
//
// Provides a mini key-value store on top of a write-ahead log that only serves
// Gets from the log (missing keys may have been migrated to SSTables)
//
// Log records are KeyUpdates, as encoded/decoded by binary.go.
// TODO: extend record format to batches of operations.

import (
	"bytes"
	"sort"

	"github.com/tchajed/specious-db/fs"
	"github.com/tchajed/specious-db/log"
)

// higher-level interface to log that supports writing operations and reading
// from a cache of the log
type dbLog struct {
	log   log.Writer
	cache entrySearchTree
}

type entrySearchTree struct {
	// works because keys are uint64s
	cache map[Key]MaybeValue
}

func newSearchTree() entrySearchTree {
	return entrySearchTree{make(map[Key]MaybeValue)}
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

func (l dbLog) logUpdate(e KeyUpdate) {
	var b bytes.Buffer
	w := newWriter(&b)
	w.KeyUpdate(e)
	l.log.Add(b.Bytes())
}

func (l dbLog) Put(k Key, v Value) {
	l.logUpdate(KeyUpdate{k, SomeValue(v)})
	l.cache.Put(k, v)
}

func (l dbLog) Delete(k Key) {
	l.logUpdate(KeyUpdate{k, NoValue})
	l.cache.Delete(k)
}

func (l dbLog) Updates() []KeyUpdate {
	return l.cache.Updates()
}

func initLog(fs fs.Filesys) dbLog {
	f := fs.Create("log")
	log := log.New(f)
	return dbLog{log, newSearchTree()}
}

func recoverUpdates(fs fs.Filesys) []KeyUpdate {
	f := fs.Open("log")
	txns := log.RecoverTxns(f)
	updates := make([]KeyUpdate, 0, len(txns))
	for _, txn := range txns {
		r := SliceReader{txn}
		e := r.KeyUpdate()
		updates = append(updates, e)
	}
	return updates
}

func (l dbLog) Close() {
	l.log.Close()
}
