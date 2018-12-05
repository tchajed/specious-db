package db

import (
	"github.com/tchajed/specious-db/fs"
)

type Database struct {
	fs  fs.Filesys
	log *dbLog
	mf  Manifest
}

func (db *Database) Get(k Key) MaybeValue {
	mv := db.log.Get(k)
	if mv.Present {
		return mv
	}
	return db.mf.Get(k)
}

func (db *Database) Put(k Key, v Value) {
	db.log.Put(k, v)
	if db.log.SizeEstimate() >= 4*1024*1024 {
		db.compactLog()
	}
}

func (db *Database) Delete(k Key) {
	db.log.Delete(k)
}

var _ Store = &Database{}

func Init(filesys fs.Filesys) *Database {
	fs.DeleteAll(filesys)
	mf := initManifest(filesys)
	log := initLog(filesys)
	return &Database{filesys, log, mf}
}

func Open(fs fs.Filesys) *Database {
	mf := newManifest(fs)
	updates := recoverUpdates(fs)
	if len(updates) > 0 {
		// save these to a table; this should be crash-safe because a
		// partially-written table will be deleted by DeleteObsoleteFiles()
		t := mf.CreateTable()
		for _, e := range updates {
			t.Put(e)
		}
		t.CloseAndInstall()
		// if we crash here, the log will be converted to a duplicate table
		//
		// NOTE: these tables will only be merged by another compaction (once
		// that's implemented)
		fs.Truncate("log")
	}
	log := initLog(fs)
	return &Database{fs, log, mf}
}

func (db *Database) compactLog() {
	db.log.Close()
	updates := db.log.Updates()
	if len(updates) == 0 {
		return
	}
	t := db.mf.CreateTable()
	for _, e := range updates {
		t.Put(e)
	}
	t.CloseAndInstall()
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

func (db *Database) Close() {
	db.compactLog()
	db.log.Close()
}
