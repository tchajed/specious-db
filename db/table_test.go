package db

import (
	"testing"

	"github.com/stretchr/testify/suite"
	"github.com/tchajed/specious-db/fs"
)

func putU(k int, v string) KeyUpdate {
	return KeyUpdate{Key(uint64(k)), SomeValue(Value(v))}
}

func deleteU(k int) KeyUpdate {
	return KeyUpdate{Key(uint64(k)), NoValue}
}

func someval(v string) MaybeMaybeValue {
	return MaybeMaybeValue{true, SomeValue(Value(v))}
}

func unknownval() MaybeMaybeValue {
	return MaybeMaybeValue{Valid: false}
}

func knowndelete() MaybeMaybeValue {
	return MaybeMaybeValue{Valid: true, MaybeValue: NoValue}
}

type TableSuite struct {
	suite.Suite
	fs fs.Filesys
	w  *tableWriter
	*Table
}

func TestTable(t *testing.T) {
	suite.Run(t, new(TableSuite))
}

func (suite *TableSuite) SetupTest() {
	suite.fs = fs.MemFs()
	f := suite.fs.Create(identToName(0))
	suite.w = newTableWriter(f)
}

// DoneWriting creates the table and opens it up for reads (with some extra
// sanity checks as tests).
//
// Tables are immutable, so tests must finish creating the table, call DoneWriting
func (suite *TableSuite) DoneWriting() {
	entries := suite.w.Close()
	t := OpenTable(0, suite.fs)
	suite.Table = &t
	suite.Require().Equal(entries, t.index.entries)
}

func (suite *TableSuite) TestTableGet() {
	suite.w.Put(putU(1, "val 1"))
	suite.w.Put(putU(2, "val 2"))
	suite.DoneWriting()
	suite.Equal(someval("val 1"), suite.Get(1))
	suite.Equal(someval("val 2"), suite.Get(2))
	suite.Equal(unknownval(), suite.Get(3))
}

func (suite *TableSuite) TestTableDelete() {
	suite.w.Put(putU(1, "val 1"))
	suite.w.Put(putU(2, "val 2"))
	suite.w.Put(deleteU(3))
	suite.DoneWriting()
	suite.Equal(knowndelete(), suite.Get(3))
	suite.Equal(unknownval(), suite.Get(7))
}

func (suite *TableSuite) TestUpdates() {
	updates := []KeyUpdate{
		putU(1, "val 1"),
		putU(2, "val 2"),
		deleteU(3),
		putU(5, "val 5"),
	}
	suite.w.Put(updates[0])
	suite.w.flush()
	suite.w.Put(updates[1])
	suite.w.Put(updates[2])
	suite.w.flush()
	suite.w.Put(updates[3])
	suite.DoneWriting()
	entries := make([]KeyUpdate, 0)
	it := suite.Updates()
	for i := 0; it.HasNext(); i++ {
		if i > len(updates) {
			suite.Fail("iterator has too many items")
			break
		}
		entries = append(entries, it.Next())
	}
	suite.Equal(updates, entries)
}
