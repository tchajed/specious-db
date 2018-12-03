package memdb

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/tchajed/specious-db/db"
)

const missing = "<missing>"

func newDb() db.StringStore {
	return db.StringStore{New()}
}

func TestPutGet(t *testing.T) {
	assert := assert.New(t)
	s := newDb()
	assert.Equal(missing, s.Get(1), "values should be missing before put")
	s.Put(1, "val")
	assert.Equal("val", s.Get(1), "should get value put")
}

func TestGet0(t *testing.T) {
	assert := assert.New(t)
	s := newDb()
	assert.Equal(missing, s.Get(0))
	s.Put(0, "val")
	assert.Equal("val", s.Get(0), "the empty key should be an ordinary key")
}

func TestDelete(t *testing.T) {
	assert := assert.New(t)
	s := newDb()
	s.Put(1, "val_1")
	s.Put(2, "val_2")
	s.Delete(1)
	assert.Equal(missing, s.Get(1), "deleted key should be missing")
	assert.Equal("val_2", s.Get(2), "non-deleted key should be present")
}

func TestDoublePut(t *testing.T) {
	assert := assert.New(t)
	s := newDb()
	s.Put(1, "val_1")
	s.Put(2, "val_2")
	s.Put(1, "val_1'")
	assert.Equal("val_1'", s.Get(1), "later puts should overwrite earlier ones")
}
