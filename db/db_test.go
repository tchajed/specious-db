package db

import (
	"github.com/stretchr/testify/suite"
	"github.com/tchajed/specious-db/fs"
)

const missing = "<missing>"

type StringStore struct{ *Database }

func (s StringStore) Get(k int) string {
	v := s.Database.Get(Key(k))
	if v.Present {
		return string(v.Value)
	}
	return missing
}

func (s StringStore) Put(k int, v string) {
	if v == missing {
		s.Database.Delete(Key(k))
	} else {
		s.Database.Put(Key(k), []byte(v))
	}
}

type DbSuite struct {
	suite.Suite
	fs fs.Filesys
	db StringStore
}

func (suite *DbSuite) SetupTest() {
	suite.fs = fs.MemFs()
	suite.db = StringStore{Init(suite.fs)}
}
