package db

import (
	"testing"

	"github.com/stretchr/testify/suite"
)

type RestartSuite struct {
	*DbSuite
	forceRestart bool
}

func TestRestartCleanlySuite(t *testing.T) {
	suite.Run(t, RestartSuite{
		DbSuite:      new(DbSuite),
		forceRestart: false})
}

func TestForceRestartSuite(t *testing.T) {
	suite.Run(t, RestartSuite{
		DbSuite:      new(DbSuite),
		forceRestart: true})
}

// Restart restarts the database, using the same file system (though note that
// the database's in-memory data structures are only garbage collected, not
// immediately de-allocated, so goroutines keep running, in-memory data
// structures continue to live, and open files continue to function against the
// current file system).
func (suite RestartSuite) Restart() {
	if !suite.forceRestart {
		suite.db.Database.Close()
	}
	suite.db.Database = Open(suite.fs)
	// suite.fs.Debug()
}

func (suite RestartSuite) TestGet() {
	suite.db.Put(1, "val")
	suite.Restart()
	suite.Equal("val", suite.db.Get(1))
}

func (suite RestartSuite) TestGetFromTable() {
	suite.db.Put(1, "val")
	suite.db.Compact()
	suite.Restart()
	suite.Equal("val", suite.db.Get(1))
}

func (suite RestartSuite) TestMultipleTables() {
	suite.db.Put(1, "val")
	suite.db.Compact()
	suite.Restart()
	suite.db.Put(2, "val 2")
	suite.db.Compact()
	suite.Restart()
	suite.Equal("val", suite.db.Get(1))
	suite.Equal("val 2", suite.db.Get(2))
}

func (suite RestartSuite) TestMultipleUpdates() {
	suite.db.Put(1, "oldest")
	suite.db.Compact()
	suite.Restart()
	suite.db.Put(1, "old")
	suite.db.Put(2, "val 2")
	suite.db.Compact()
	suite.Restart()
	suite.db.Put(1, missing)
	suite.db.Compact()
	suite.Equal(missing, suite.db.Get(1))
	suite.Equal("val 2", suite.db.Get(2))
}

func (suite RestartSuite) TestIndexing() {
	suite.db.Put(0, "table min")
	suite.db.Put(1000, "table max")
	suite.putValues(1, 100)
	suite.db.Compact()
	suite.db.Put(0, "table min")
	suite.db.Put(1001, "table max")
	suite.putValues(101, 200)
	suite.Restart()
	suite.checkKey(10, "value from table 1")
	suite.checkKey(110, "value from table 1")
	suite.Equal("table max", suite.db.Get(1000), "max from table 1")
	suite.Equal("table max", suite.db.Get(1001), "max from table 2")
	suite.Equal(missing, suite.db.Get(10000), "non-existent key")
}

func (suite RestartSuite) TestOutOfOrderWrites() {
	suite.db.Put(0, "val 0")
	suite.db.Put(2, "val 2")
	suite.db.Put(1, "val 1")
	suite.Restart()
	suite.Equal("val 2", suite.db.Get(2), "out of order write")
}
