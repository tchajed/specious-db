package db

import (
	"fmt"
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

func (suite CompactSuite) testGet() {
	suite.db.Put(1, "val 1")
	suite.db.Compact()
	suite.Equal("val 1", suite.db.Get(1))
}

func (suite *DbSuite) putValues(min, max int) {
	for i := min; i <= max; i++ {
		suite.db.Put(i, fmt.Sprintf("val %d", i))
	}
}

// TODO: we could check the database against an in-memory version rather than
// hard-coding expected results
func (suite *DbSuite) checkKey(key int, msgAndArgs ...interface{}) {
	suite.Equal(fmt.Sprintf("val %d", key), suite.db.Get(key), msgAndArgs...)
}

func (suite CompactSuite) TestIndexing() {
	suite.db.Put(0, "table min")
	suite.db.Put(1000, "table max")
	suite.putValues(1, 100)
	suite.db.Compact()
	suite.db.Put(0, "table min")
	suite.db.Put(1001, "table max")
	suite.putValues(101, 200)
	suite.db.Compact()
	suite.checkKey(10, "value from table 1")
	suite.checkKey(110, "value from table 1")
	suite.Equal("table max", suite.db.Get(1000), "max from table 1")
	suite.Equal("table max", suite.db.Get(1001), "max from table 2")
	suite.Equal(missing, suite.db.Get(10000), "non-existent key")
}
