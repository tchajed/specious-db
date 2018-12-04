package memdb

import "github.com/tchajed/specious-db/db"

type Memdb struct {
	m map[db.Key]db.Value
}

func (s Memdb) Get(k db.Key) db.MaybeValue {
	val, ok := s.m[k]
	if !ok {
		return db.NoValue
	}
	return db.SomeValue(val)
}

func (s *Memdb) Put(k db.Key, v db.Value) {
	s.m[k] = v
}

func (s *Memdb) Delete(k db.Key) {
	delete(s.m, k)
}

func (s *Memdb) Close() {
}

func New() *Memdb {
	return &Memdb{make(map[db.Key]db.Value)}
}
