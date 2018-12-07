package db

import (
	"testing"

	"github.com/stretchr/testify/suite"
)

const (
	noRestart int = iota
	compactOnlyLog
	compactAll
	forceRestart
	cleanRestart
)

type RestartSuite struct {
	*DbSuite
	restartType int
}

func TestNoRestartSuite(t *testing.T) {
	suite.Run(t, RestartSuite{
		DbSuite:     new(DbSuite),
		restartType: noRestart})
}

func TestCompactLogSuite(t *testing.T) {
	suite.Run(t, RestartSuite{
		DbSuite:     new(DbSuite),
		restartType: compactOnlyLog})
}

func TestCompactAllSuite(t *testing.T) {
	suite.Run(t, RestartSuite{
		DbSuite:     new(DbSuite),
		restartType: compactAll})
}

func TestRestartCleanlySuite(t *testing.T) {
	suite.Run(t, RestartSuite{
		DbSuite:     new(DbSuite),
		restartType: cleanRestart})
}

func TestForceRestartSuite(t *testing.T) {
	suite.Run(t, RestartSuite{
		DbSuite:     new(DbSuite),
		restartType: forceRestart})
}

// Restart restarts the database, using the same file system (though note that
// the database's in-memory data structures are only garbage collected, not
// immediately de-allocated, so goroutines keep running, in-memory data
// structures continue to live, and open files continue to function against the
// current file system).
func (suite RestartSuite) Restart() {
	switch suite.restartType {
	case noRestart:
	case compactOnlyLog:
		suite.db.compactLog()
	case compactAll:
		suite.db.Compact()
	case forceRestart:
		suite.db.Database = Open(suite.fs)
	case cleanRestart:
		suite.db.Database.Close()
		suite.db.Database = Open(suite.fs)
	}
	// fs.Debug(suite.fs)
}

func (suite RestartSuite) TestGet() {
	suite.db.Put(1, "val")
	suite.Restart()
	suite.check(1)
}

func (suite RestartSuite) TestGetFromTable() {
	suite.db.Put(1, "val")
	suite.Restart()
	suite.check(1)
}

func (suite RestartSuite) TestMultipleTables() {
	suite.db.Put(1, "val")
	suite.Restart()
	suite.db.Put(2, "val 2")
	suite.Restart()
	suite.check(1)
	suite.check(2)
}

func (suite RestartSuite) TestMultipleUpdates() {
	suite.db.Put(1, "oldest")
	suite.Restart()
	suite.db.Put(1, "old")
	suite.db.Put(2, "val 2")
	suite.Restart()
	suite.db.Put(1, missing)
	suite.check(1)
	suite.check(2)
}

func (suite RestartSuite) TestIndexing() {
	suite.db.Put(0, "table min")
	suite.db.Put(1000, "table max")
	suite.putValues(1, 100)
	suite.db.Put(0, "table min")
	suite.db.Put(1001, "table max")
	suite.putValues(101, 200)
	suite.Restart()
	suite.check(10, "value from table 1")
	suite.check(110, "value from table 1")
	suite.check(1000, "max from table 1")
	suite.check(1001, "max from table 2")
	suite.check(10000, "other key")
}

func (suite RestartSuite) TestOutOfOrderWrites() {
	suite.db.Put(0, "val 0")
	suite.db.Put(2, "val 2")
	suite.db.Put(1, "val 1")
	suite.Restart()
	suite.check(2, "out-of-order write")
}
