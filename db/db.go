package db

import (
	"errors"

	"github.com/tchajed/specious-db/fs"
)

type Key = []byte
type Value = []byte

func KeyEq(k1 Key, k2 Key) bool {
	if len(k1) != len(k2) {
		return false
	}
	for i := range k1 {
		if k1[i] != k2[i] {
			return false
		}
	}
	return true
}

type ErrKeyMissing struct{}

func (e ErrKeyMissing) Error() string {
	return "no such key"
}

type Store interface {
	Get(k Key) (Value, error)
	Put(k Key, v Value) error
	Delete(k Key) error
	// TODO: iterator API
}

type Database struct {
	fs fs.Filesys
}

func (db *Database) Get(k Key) (Value, error) {
	return nil, ErrKeyMissing{}
}

func (db *Database) Put(k Key, v Value) error {
	return errors.New("not implemented")
}

func (db *Database) Delete(Key) error {
	return nil
}

var _ Store = &Database{}

func New(fs fs.Filesys) *Database {
	return &Database{fs}
}
