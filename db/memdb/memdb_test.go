package memdb

import "testing"
import "github.com/stretchr/testify/assert"
import "github.com/tchajed/specious-db/db"

type StringStore struct {store db.Store}

func (s StringStore) Get(k string) string {
	v, err := s.store.Get([]byte(k))
	if err == nil {
		return string(v)
	}
	switch err.(type) {
	case db.ErrKeyMissing:
		return ""
	default:
		panic(err)
	}
}

func (s StringStore) Put(k string, v string) {
	err := s.store.Put([]byte(k), []byte(v))
	if err != nil {
		panic(err)
	}
}

func (s StringStore) Delete(k string) {
	err := s.store.Delete([]byte(k))
	if err != nil {
		panic(err)
	}
}

func TestPutGet(t *testing.T) {
	assert := assert.New(t)
	s := StringStore{New()}
	assert.Equal(s.Get("a"), "", "values should be missing before put")
	s.Put("a", "val")
	assert.Equal(s.Get("a"), "val", "should get value put")
}

func TestGetEmpty(t *testing.T) {
	assert := assert.New(t)
	s := StringStore{New()}
	assert.Equal(s.Get(""), "")
	s.Put("", "val")
	assert.Equal(s.Get(""), "", "the empty key should never be stored")
}

func TestDelete(t *testing.T) {
	assert := assert.New(t)
	s := StringStore{New()}
	s.Put("a", "val_a")
	s.Put("b", "val_b")
	s.Delete("a")
	assert.Equal(s.Get("a"), "", "deleted key should be missing")
	assert.Equal(s.Get("b"), "val_b", "non-deleted key should be present")
}

func TestDoublePut(t *testing.T) {
	assert := assert.New(t)
	s := StringStore{New()}
	s.Put("a", "val_a")
	s.Put("b", "val_b")
	s.Put("a", "val_a2")
	assert.Equal(s.Get("a"), "val_a2", "later puts should overwrite earlier ones")
}
