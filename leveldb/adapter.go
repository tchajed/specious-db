package leveldb

import (
	"encoding/binary"
	"sync"

	"github.com/jmhodges/levigo"
	"github.com/tchajed/specious-db/db"
)

// Database is a wrapper around a LevelDB database
type Database struct {
	db *levigo.DB
	// scratch space to create key buffers
	keyDataPool *sync.Pool
	wo          *levigo.WriteOptions
}

func newKeyDataPool() *sync.Pool {
	return &sync.Pool{New: func() interface{} {
		return make([]byte, 8)
	}}
}

func levelDbOpts() *levigo.Options {
	opts := levigo.NewOptions()
	opts.SetCreateIfMissing(true)
	opts.SetCompression(levigo.NoCompression)

	// performance-related configuration
	cache := levigo.NewLRUCache(0)
	opts.SetCache(cache)
	// 4MB is the default
	opts.SetWriteBufferSize(4 * 1024 * 1024)

	return opts
}

// New creates a LevelDB instance at path.
//
// Creates the path if it does not exist.
func New(path string) *Database {
	db, err := levigo.Open(path, levelDbOpts())
	if err != nil {
		panic(err)
	}
	pool := newKeyDataPool()
	wo := levigo.NewWriteOptions()
	return &Database{db, pool, wo}
}

// fromDbKey converts a db.Key (a uint64) to a byte slice for usage with leveldb
func (d Database) fromDbKey(k db.Key) []byte {
	keyScratch := d.keyDataPool.Get().([]byte)
	binary.LittleEndian.PutUint64(keyScratch, uint64(k))
	return keyScratch
}

// Get retrieves a key from the database.
func (d Database) Get(k db.Key) db.MaybeValue {
	ro := levigo.NewReadOptions()
	data, err := d.db.Get(ro, d.fromDbKey(k))
	if err != nil {
		panic(err)
	}
	if data == nil {
		return db.NoValue
	}
	return db.SomeValue(data)
}

// Put inserts a key into the database.
func (d Database) Put(k db.Key, v db.Value) {
	err := d.db.Put(d.wo, d.fromDbKey(k), v)
	if err != nil {
		panic(err)
	}
}

// Delete deletes a key from the database.
func (d Database) Delete(k db.Key) {
	err := d.db.Delete(d.wo, d.fromDbKey(k))
	if err != nil {
		panic(err)
	}
}

// Close shuts down the database.
func (d Database) Close() {
	d.wo.Close()
	d.db.Close()
}

// Compact runs log and sstable compaction.
func (d Database) Compact() {
	d.db.CompactRange(levigo.Range{})
}
