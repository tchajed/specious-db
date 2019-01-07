package memdb

import (
	"sync"

	"github.com/tchajed/specious-db/db"
)

// Database is an in-memory, non-persistent database mainly for testing purposes.
type Database struct {
	l *sync.Mutex
	m map[db.Key]db.Value
}

// Get looks up a key.
func (s Database) Get(k db.Key) db.MaybeValue {
	s.l.Lock()
	defer s.l.Unlock()
	val, ok := s.m[k]
	if !ok {
		return db.NoValue
	}
	return db.SomeValue(val)
}

// Put stores data in the database.
func (s *Database) Put(k db.Key, v db.Value) {
	s.l.Lock()
	defer s.l.Unlock()
	s.m[k] = v
}

// Delete deletes a key.
func (s *Database) Delete(k db.Key) {
	s.l.Lock()
	defer s.l.Unlock()
	delete(s.m, k)
}

// Close does nothing
func (s *Database) Close() {}

// Compact does nothing. In-memory databases are always compact.
func (s *Database) Compact() {}

// New creates an empty in-memory database.
func New() *Database {
	return &Database{l: new(sync.Mutex), m: make(map[db.Key]db.Value)}
}
