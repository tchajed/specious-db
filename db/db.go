package db

import (
	"github.com/tchajed/specious-db/fs"
)

type Database struct {
	log dbLog
	fs  fs.Filesys
}

func (db *Database) Get(k Key) MaybeValue {
	return db.log.Get(k).OrElse(func() MaybeValue {
		// not found in log
		// TODO: search SSTables via manifest
		panic("sstables not implemented")
	})
}

func (db *Database) Put(k Key, v Value) {
	db.log.Put(k, v)
}

func (db *Database) Delete(k Key) {
	db.log.Delete(k)
}

var _ Store = &Database{}

func Init(fs fs.Filesys) *Database {
	initManifest(fs)
	log := initLog(fs)
	return &Database{log, fs}
}

func New(fs fs.Filesys) *Database {
	log := recoverLog(fs)
	return &Database{log, fs}
}
