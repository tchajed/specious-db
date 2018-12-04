package db

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
)

type CompactSuite struct {
	*DbSuite
}

func TestKeyContains(t *testing.T) {
	assert.True(t, KeyRange{1, 1}.Contains(1))
}

func TestCompactSuite(t *testing.T) {
	suite.Run(t, CompactSuite{new(DbSuite)})
}

func (suite *DbSuite) Tables() []Table {
	return suite.db.mf.tables
}

func (suite CompactSuite) TestTableRangeSingleton() {
	suite.db.Put(1, "val 1")
	suite.db.Compact()
	suite.Equal(KeyRange{1, 1}, suite.Tables()[0].Keys())
}

func (suite CompactSuite) TestTableRangeMultiple() {
	suite.db.Put(3, "val 2")
	suite.db.Put(1, "val 1")
	suite.db.Compact()
	suite.Equal(KeyRange{1, 3}, suite.Tables()[0].Keys())
}

func (suite CompactSuite) TestGetHandle() {
	suite.db.Put(1, "val 1")
	suite.db.Compact()
	h := suite.Tables()[0].index.Get(1)
	suite.True(h.IsValid(), "table index has invalid handle %#v", h)
}

func (suite CompactSuite) TestGetFromTable() {
	suite.db.Put(1, "val 1")
	suite.db.Compact()
	suite.Equal(
		MaybeMaybeValue{true, MaybeValue{Present: true, Value: []byte("val 1")}},
		suite.Tables()[0].Get(1),
		"only table should have key")
}

func (suite CompactSuite) TestCompactEmptyLog() {
	suite.db.Compact()
}
