package memdb

import "testing"
import "github.com/stretchr/testify/assert"
import "github.com/tchajed/specious-db/db"

type StringStore struct{ store db.Store }

func (s StringStore) Get(k int) string {
	v := s.store.Get(db.Key(k))
	if v.Present {
		return string(v.Value)
	}
	return ""
}

func (s StringStore) Put(k int, v string) {
	s.store.Put(db.Key(k), []byte(v))
}

func (s StringStore) Delete(k int) {
	s.store.Delete(db.Key(k))
}

func TestPutGet(t *testing.T) {
	assert := assert.New(t)
	s := StringStore{New()}
	assert.Equal(s.Get(1), "", "values should be missing before put")
	s.Put(1, "val")
	assert.Equal(s.Get(1), "val", "should get value put")
}

func TestGet0(t *testing.T) {
	assert := assert.New(t)
	s := StringStore{New()}
	assert.Equal(s.Get(0), "")
	s.Put(0, "val")
	assert.Equal(s.Get(0), "val", "the empty key should be an ordinary key")
}

func TestDelete(t *testing.T) {
	assert := assert.New(t)
	s := StringStore{New()}
	s.Put(1, "val_1")
	s.Put(2, "val_2")
	s.Delete(1)
	assert.Equal(s.Get(1), "", "deleted key should be missing")
	assert.Equal(s.Get(2), "val_2", "non-deleted key should be present")
}

func TestDoublePut(t *testing.T) {
	assert := assert.New(t)
	s := StringStore{New()}
	s.Put(1, "val_1")
	s.Put(2, "val_2")
	s.Put(1, "val_1'")
	assert.Equal(s.Get(1), "val_1'", "later puts should overwrite earlier ones")
}
