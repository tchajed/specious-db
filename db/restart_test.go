package db

import (
	"testing"

	"github.com/stretchr/testify/suite"
)

type RestartSuite struct {
	*DbSuite
}

func TestRestartSuite(t *testing.T) {
	suite.Run(t, RestartSuite{new(DbSuite)})
}

// Restart forcibly restarts the database, using the same file system (though
// note that the databse is only garbage collected, not truly shut down, so
// goroutines keep running, in-memory data structures continue to live, and open
// files continue to function against the current file system).
func (suite RestartSuite) Restart() {
	suite.db.Database = New(suite.fs)
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
