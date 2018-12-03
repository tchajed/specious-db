package db

import (
	"github.com/tchajed/specious-db/fs"
)

type Database struct {
	fs  fs.Filesys
	log dbLog
	mf  Manifest
}

func (db *Database) Get(k Key) MaybeValue {
	return db.log.Get(k).OrElse(func() MaybeValue {
		return db.mf.Get(k)
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
	return &Database{fs, log, mf}
}

func New(fs fs.Filesys) *Database {
	mf := newManifest(fs)
	updates := recoverUpdates(fs)
	if len(updates) > 0 {
		// save these to a table; this should be crash-safe because a
		// partially-written table will be deleted by DeleteObsoleteFiles()
		t := mf.NewTable()
		for _, e := range updates {
			t.Put(e)
		}
		mf.InstallTable(t.Build())
	}
	// TODO: see below comment about atomically installing table and deleting
	// log
	fs.Truncate("log")
	log := initLog(fs)
	return &Database{fs, log, mf}
}

func (db *Database) compactLog() {
	db.log.Close()
	updates := db.log.Updates()
	if len(updates) == 0 {
		return
	}
	t := db.mf.NewTable()
	for _, e := range updates {
		t.Put(e)
	}
	db.mf.InstallTable(t.Build())
	// TODO: if we crash here, recovery needs to see from the manifest that the
	// log has been installed and therefore to delete it (it could technically
	// be in multiple tables, but that will be confusing to prove correct)
	db.fs.Truncate("log")
	db.log = initLog(db.fs)
}

func (db *Database) DeleteObsoleteFiles() {
	db.mf.cleanup()
}

func (db *Database) Compact() {
	db.compactLog()
	db.DeleteObsoleteFiles()
}
