package memdb

import "github.com/tchajed/specious-db/db"

type Memdb struct {
	entries []db.Entry
}

func (s Memdb) Get(k db.Key) db.MaybeValue {
	val := db.MaybeValue{Present: false}
	for _, e := range s.entries {
		if db.KeyEq(e.Key, k) {
			val = db.SomeValue(e.Value)
		}
	}
	return val
}

func (s *Memdb) Put(k db.Key, v db.Value) {
	s.entries = append(s.entries, db.Entry{k, v})
}

func (s *Memdb) Delete(k db.Key) {
	for i, e := range s.entries {
		if db.KeyEq(e.Key, k) {
			s.entries[i] = db.Entry{0, nil}
		}
	}
	return
}

var _ db.Store = &Memdb{}

func New() *Memdb {
	return &Memdb{}
}
