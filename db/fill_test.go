package db

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/suite"
)

type FillSuite struct {
	*DbSuite
}

func TestFillSuite(t *testing.T) {
	suite.Run(t, FillSuite{new(DbSuite)})
}

func (suite FillSuite) put(key int) {
	suite.db.Put(key, fmt.Sprintf("val %d", key))
}

func (suite FillSuite) TestFillDatabase() {
	suite.put(1)
	suite.put(7)
	suite.db.compactLog()
	suite.put(2)
	suite.put(5)
	suite.db.compactLog()
	suite.put(10)
	suite.put(3)
	suite.db.compactLog()
	suite.put(12)
	suite.db.compactYoung()
	for key := 0; key < 12; key++ {
		suite.check(key)
	}
	suite.Equal(0, len(suite.db.mf.tables[0]),
		"young tables should be deleted")
	suite.Equal(1, len(suite.db.mf.tables[1]),
		"all data should be in single level 1 table")
}
