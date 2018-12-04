package leveldb

import (
	"encoding/binary"
	"sync"

	"github.com/jmhodges/levigo"
	"github.com/tchajed/specious-db/db"
)

type levelDb struct {
	*levigo.DB
	// scratch space to create key buffers
	keyDataPool *sync.Pool
}

func newKeyDataPool() *sync.Pool {
	return &sync.Pool{New: func() interface{} {
		return make([]byte, 8)
	}}
}

// New creates a LevelDB instance at path.
//
// Creates the path if it does not exist.
func New(path string) db.Store {
	opts := levigo.NewOptions()
	opts.SetCreateIfMissing(true)
	opts.SetCompression(levigo.NoCompression)
	// NOTE: leveldb may truncate the cache size to something higher
	cache := levigo.NewLRUCache(0)
	opts.SetCache(cache)
	// NOTE: leveldb may have a minimum write buffer size
	opts.SetWriteBufferSize(0)
	db, err := levigo.Open(path, opts)
	if err != nil {
		panic(err)
	}
	return levelDb{db, newKeyDataPool()}
}

// fromDbKey converts a db.Key (a uint64) to a byte slice for usage with leveldb
func (d levelDb) fromDbKey(k db.Key) []byte {
	keyScratch := d.keyDataPool.Get().([]byte)
	binary.LittleEndian.PutUint64(keyScratch, k)
	return keyScratch
}

func (d levelDb) Get(k db.Key) db.MaybeValue {
	ro := levigo.NewReadOptions()
	data, err := d.DB.Get(ro, d.fromDbKey(k))
	if err != nil {
		panic(err)
	}
	if data == nil {
		return db.NoValue
	}
	return db.SomeValue(data)
}

func (d levelDb) Put(k db.Key, v db.Value) {
	wo := levigo.NewWriteOptions()
	err := d.DB.Put(wo, d.fromDbKey(k), v)
	if err != nil {
		panic(err)
	}
}

func (d levelDb) Delete(k db.Key) {
	wo := levigo.NewWriteOptions()
	err := d.DB.Delete(wo, d.fromDbKey(k))
	if err != nil {
		panic(err)
	}
}

func (d levelDb) Close() {
	d.DB.Close()
}
