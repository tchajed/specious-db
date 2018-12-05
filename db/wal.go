package db

// Key-value store logging structure
//
// Provides a mini key-value store on top of a write-ahead log that only serves
// Gets from the log (missing keys may have been migrated to SSTables)
//
// Log records are sequences of KeyUpdates.

// NOTE: log format supports multiple key updates in a log record, but the
// external interface doesn't provides this (the low-level logUpdates supports
// logging transactional writes and recoverUpdates will correctly handle
// multiple updates in one record).

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
	// an estimate of how big the log is (tracks puts, but does not account for
	// encoding overhead or subtract for coalesced update)
	sizeBytes int
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

func sortUpdates(es []KeyUpdate) {
	sort.Slice(es, func(i, j int) bool { return es[i].Key < es[j].Key })
}

func (t entrySearchTree) Updates() []KeyUpdate {
	updates := make([]KeyUpdate, 0, len(t.cache))
	for k, ku := range t.cache {
		updates = append(updates, KeyUpdate{k, ku})
	}
	sortUpdates(updates)
	return updates
}

func (l dbLog) Get(k Key) MaybeValue {
	return l.cache.Get(k)
}

func (l dbLog) logUpdates(es []KeyUpdate) {
	b := bytes.NewBuffer(make([]byte, 0, 8+len(es[0].Value)))
	w := newEncoder(b)
	for _, e := range es {
		w.KeyUpdate(e)
	}
	l.log.Add(b.Bytes())
}

func (l *dbLog) Put(k Key, v Value) {
	l.logUpdates([]KeyUpdate{{k, SomeValue(v)}})
	l.cache.Put(k, v)
	l.sizeBytes += 8 + len(v)
}

func (l dbLog) Delete(k Key) {
	l.logUpdates([]KeyUpdate{{k, NoValue}})
	l.cache.Delete(k)
}

func (l dbLog) Updates() []KeyUpdate {
	return l.cache.Updates()
}

func (l dbLog) SizeEstimate() int {
	return l.sizeBytes
}

func initLog(fs fs.Filesys) *dbLog {
	f := fs.Create("log")
	log := log.New(f)
	return &dbLog{log, newSearchTree(), 0}
}

func recoverUpdates(fs fs.Filesys) []KeyUpdate {
	f := fs.Open("log")
	txns := log.RecoverTxns(f)
	f.Close()
	updates := make([]KeyUpdate, 0, len(txns))
	for _, txn := range txns {
		r := newDecoder(txn)
		for r.RemainingBytes() > 0 {
			e := r.KeyUpdate()
			updates = append(updates, e)
		}
	}
	sortUpdates(updates)
	return updates
}

func (l dbLog) Close() {
	l.log.Close()
}
