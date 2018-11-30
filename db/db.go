package db

import (
	"github.com/tchajed/specious-db/fs"
)

type Database struct {
	log dbLog
	mf  Manifest
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
	mf := initManifest(fs)
	log := initLog(fs)
	return &Database{log, mf, fs}
}

func New(fs fs.Filesys) *Database {
	log := recoverLog(fs)
	mf := newManifest(fs)
	return &Database{log, mf, fs}
}

func (db *Database) CompactLog() {
	it := db.log.Stream()
	t := db.mf.NewTable(0)
	for !it.IsDone() {
		e := it.Next()
		t.Put(e)
	}
	db.mf.InstallTable(t.Build())
	db.log.Close()
	db.log = initLog(db.fs)
}
