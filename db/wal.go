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
	cache map[Key]Value
}

func (t entrySearchTree) Get(k Key) MaybeValue {
	v, ok := t.cache[k]
	if ok {
		return SomeValue(v)
	} else {
		return NoValue
	}
}

func (t entrySearchTree) Put(k Key, v Value) {
	t.cache[k] = v
}

func (t entrySearchTree) Delete(k Key) {
	delete(t.cache, k)
}

type inOrderIterator struct {
	entries []Entry
}

func (it inOrderIterator) Len() int {
	return len(it.entries)
}

func (it inOrderIterator) Less(i, j int) bool {
	return it.entries[i].Key < it.entries[j].Key
}

func (it inOrderIterator) Swap(i, j int) {
	it.entries[i], it.entries[j] = it.entries[j], it.entries[i]
}

// Create an iterator over a list of entries and put it in order;
// consumes the input slice (because it's sorted in-place).
func sortedIterator(entries []Entry) *inOrderIterator {
	it := inOrderIterator{entries}
	sort.Sort(it)
	return &it
}

func (it inOrderIterator) IsDone() bool {
	return len(it.entries) == 0
}

func (it *inOrderIterator) Next() Entry {
	e := it.entries[0]
	it.entries = it.entries[1:]
	return e
}

func (t entrySearchTree) Stream() EntryIterator {
	// NOTE: this stream already colaesces all updates to the same key
	//
	// TODO: this is actually insufficient; need to record deletions so two log
	// files can be concatenated (though recording only deletions and clearing
	// the deletions on Put is sufficient)
	entries := make([]Entry, 0, len(t.cache))
	for k, v := range t.cache {
		entries = append(entries, Entry{k, v})
	}
	return sortedIterator(entries)
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

func (l dbLog) Close() {
	l.log.Close()
}
