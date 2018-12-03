package db

import "testing"
import "github.com/stretchr/testify/suite"

type PutGetSuite struct {
	*DbSuite
}

func TestPutGetSuite(t *testing.T) {
	suite.Run(t, PutGetSuite{new(DbSuite)})
}

func (suite PutGetSuite) TestPutGet() {
	suite.db.Put(1, "val")
	suite.Equal("val", suite.db.Get(1))
}

func (suite PutGetSuite) TestGetMissing() {
	suite.Equal(missing, suite.db.Get(1))
}

func (suite PutGetSuite) TestPutReplace() {
	suite.Equal(missing, suite.db.Get(1))
	suite.db.Put(1, "val")
	suite.Equal("val", suite.db.Get(1))
	suite.db.Put(1, "new val")
	suite.Equal("new val", suite.db.Get(1))
}

func (suite PutGetSuite) TestPutDelete() {
	suite.db.Put(1, "val")
	suite.db.Put(2, "val 2")
	suite.Equal("val", suite.db.Get(1))
	suite.db.Put(1, missing)
	suite.Equal(missing, suite.db.Get(1))
	suite.Equal("val 2", suite.db.Get(2))
}

func (suite PutGetSuite) TestPutOther() {
	suite.Equal(missing, suite.db.Get(1))
	suite.db.Put(1, "val 1")
	suite.db.Put(2, "val 2")
	suite.Equal("val 1", suite.db.Get(1))
	suite.Equal("val 2", suite.db.Get(2))
}
