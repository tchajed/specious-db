package db

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	"github.com/tchajed/specious-db/fs"
)

const missing = "<missing>"

type StringStore struct {
	*Database
	gold map[int]string
}

func (s StringStore) Get(k int) string {
	v := s.Database.Get(Key(k))
	if v.Present {
		return string(v.Value)
	}
	return missing
}

func (s StringStore) Expected(k int) string {
	v, ok := s.gold[k]
	if ok {
		return v
	}
	return missing
}

func (s StringStore) Put(k int, v string) {
	if v == missing {
		s.Database.Delete(Key(k))
		delete(s.gold, k)
	} else {
		s.Database.Put(Key(k), []byte(v))
		s.gold[k] = v
	}
}

func newStringStore(db *Database) StringStore {
	return StringStore{db, make(map[int]string)}
}

type DbSuite struct {
	suite.Suite
	fs fs.Filesys
	db StringStore
}

func (suite *DbSuite) SetupTest() {
	suite.fs = fs.MemFs()
	suite.db = newStringStore(Init(suite.fs))
}

func (suite *DbSuite) putValues(min, max int) {
	for i := min; i <= max; i++ {
		suite.db.Put(i, fmt.Sprintf("val %d", i))
	}
}

func (suite *DbSuite) check(key int, msgAndArgs ...interface{}) {
	suite.Equal(suite.db.Expected(key), suite.db.Get(key), msgAndArgs...)
}

func TestDoubleInit(t *testing.T) {
	assert := assert.New(t)
	fs := fs.MemFs()
	db := newStringStore(Init(fs))
	db.Put(1, "val 1")
	db.Close()
	db = newStringStore(Init(fs))
	assert.Equal(missing, db.Get(1))
}
