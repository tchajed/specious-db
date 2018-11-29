package memdb

import "github.com/tchajed/specious-db/db"

type Memdb struct {
	entries []db.Entry
}

func (s Memdb) Get(k db.Key) (val db.Value, err error) {
	if len(k) == 0 {
		return nil, db.ErrKeyMissing{}
	}
	found := false
	for _, e := range s.entries {
		if db.KeyEq(e.Key, k) {
			val = e.Value
			found = true
		}
	}
	if !found {
		err = db.ErrKeyMissing{}
	}
	return
}

func (s *Memdb) Put(k db.Key, v db.Value) error {
	if len(k) == 0 {
		return nil
	}
	s.entries = append(s.entries, db.Entry{k, v})
	return nil
}

func (s *Memdb) Delete(k db.Key) (err error) {
	for i, e := range s.entries {
		if db.KeyEq(e.Key, k) {
			s.entries[i] = db.Entry{nil, nil}
		}
	}
	return
}

var _ db.Store = &Memdb{}

func New() *Memdb {
	return &Memdb{}
}
