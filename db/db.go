package db

import (
	"fmt"

	"github.com/tchajed/specious-db/fs"
)

type Database struct {
	fs  fs.Filesys
	log dbLog
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

func Open(fs fs.Filesys) *Database {
	mf, logTruncated := newManifest(fs)
	if !logTruncated {
		fmt.Println("finishing log truncation")
		fs.Truncate("log")
		mf.MarkLogTruncated()
	} else {
		updates := recoverUpdates(fs)
		if len(updates) > 0 {
			// save these to a table; this should be crash-safe because a
			// partially-written table will be deleted by DeleteObsoleteFiles()
			t := mf.CreateTable()
			for _, e := range updates {
				t.Put(e)
			}
			t.CloseAndInstall()
			fs.Truncate("log")
			mf.MarkLogTruncated()
		}
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
	db.mf.MarkLogTruncated()
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
