package memdb

import "github.com/tchajed/specious-db/db"

type Database struct {
	m map[db.Key]db.Value
}

func (s Database) Get(k db.Key) db.MaybeValue {
	val, ok := s.m[k]
	if !ok {
		return db.NoValue
	}
	return db.SomeValue(val)
}

func (s *Database) Put(k db.Key, v db.Value) {
	s.m[k] = v
}

func (s *Database) Delete(k db.Key) {
	delete(s.m, k)
}

func (s *Database) Close() {}

func (s *Database) Compact() {}

func New() *Database {
	return &Database{make(map[db.Key]db.Value)}
}
