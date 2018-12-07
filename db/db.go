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
	if mv.Valid {
		return mv.MaybeValue
	}
	return db.mf.Get(k)
}

func (db *Database) Put(k Key, v Value) {
	db.log.Put(k, v)
	if db.log.SizeEstimate() >= 4*1024*1024 {
		db.compactLog()
	}
	if len(db.mf.tables[0]) >= 4 {
		db.compactYoung()
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
	mf := recoverManifest(fs)
	updates := recoverUpdates(fs)
	if len(updates) > 0 {
		// save these to a table; this should be crash-safe because a
		// partially-written table will be deleted by DeleteObsoleteFiles()
		t := mf.CreateTable(nil, nil)
		for _, e := range updates {
			t.Put(e)
		}
		t.CloseAndInstall(0)
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
	t := db.mf.CreateTable(nil, nil)
	for _, e := range updates {
		t.Put(e)
	}
	t.CloseAndInstall(0)
	db.fs.Truncate("log")
	db.log = initLog(db.fs)
}

func (db *Database) compactYoung() {
	var youngTables []uint32
	var level1Tables []uint32
	var updateIterators []UpdateIterator
	for _, t := range db.mf.tables[0] {
		youngTables = append(youngTables, t.ident)
		updateIterators = append(updateIterators, t.Updates())
	}
	// get overlapping tables
	for _, t := range db.mf.tables[1] {
		level1Tables = append(level1Tables, t.ident)
		updateIterators = append(updateIterators, t.Updates())
	}
	t := db.mf.CreateTable(youngTables, level1Tables)
	it := MergeUpdates(updateIterators)
	for it.HasNext() {
		t.Put(it.Next())
	}
	t.CloseAndInstall(1)
}

func (db *Database) DeleteObsoleteFiles() {
	db.mf.cleanup()
}

func (db *Database) Compact() {
	db.compactLog()
	db.compactYoung()
}

func (db *Database) Close() {
	db.compactLog()
	db.log.Close()
}
