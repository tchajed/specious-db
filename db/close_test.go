package db

import (
	"testing"

	"github.com/stretchr/testify/suite"
)

type CloseSuite struct {
	*DbSuite
}

func TestCloseSuite(t *testing.T) {
	suite.Run(t, CloseSuite{new(DbSuite)})
}

func (suite CloseSuite) TestClose() {
	suite.db.Put(1, "val")
	suite.Equal("val", suite.db.Get(1))
	suite.check(1)
	suite.db.Close()
}
