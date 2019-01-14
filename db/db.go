package db

import (
	"sync"
	"time"

	"github.com/tchajed/specious-db/fs"
)

type CompactionStats struct {
	TotalTime time.Duration
}

func (stats *CompactionStats) AddTimeSince(start time.Time) {
	stats.TotalTime += time.Now().Sub(start)
}

// A Database is a persistent key-value store.
type Database struct {
	fs    fs.Filesys
	log   *dbLog
	mf    Manifest
	Stats *CompactionStats
	l     *sync.RWMutex
}

func (db *Database) Get(k Key) MaybeValue {
	db.l.RLock()
	defer db.l.RUnlock()
	mv := db.log.Get(k)
	if mv.Valid {
		return mv.MaybeValue
	}
	return db.mf.Get(k)
}

func (db *Database) Put(k Key, v Value) {
	db.l.Lock()
	defer db.l.Unlock()

	db.log.Put(k, v)
	if db.log.SizeEstimate() >= 4*1024*1024 {
		db.compactLog()
	}
	if len(db.mf.tables[0]) >= 4 {
		db.compactYoung()
	}
}

func (db *Database) Delete(k Key) {
	db.l.Lock()
	defer db.l.Unlock()

	db.log.Delete(k)
}

var _ Store = &Database{}

// Init creates a new database in a filesystem, replacing anything in the
// directory.
func Init(filesys fs.Filesys) *Database {
	fs.DeleteAll(filesys)
	mf := initManifest(filesys)
	log := initLog(filesys)
	return &Database{filesys, log, mf, new(CompactionStats), new(sync.RWMutex)}
}

// Open recovers a Database from an existing on-disk database (of course this
// also works following a clean shutdown).
func Open(fs fs.Filesys) *Database {
	mf := recoverManifest(fs)
	updates := recoverUpdates(fs)
	if len(updates) > 0 {
		// save these to a table; this should be crash-safe because a
		// partially-written table will be deleted by DeleteObsoleteFiles()
		t := mf.CreateTable()
		for _, e := range updates {
			t.Put(e)
		}
		mf.InstallTable(t.Close(), nil, nil, 0)
		// if we crash here, the log will be converted to a duplicate table
		fs.Truncate("log")
	}
	log := initLog(fs)
	return &Database{fs, log, mf, new(CompactionStats), new(sync.RWMutex)}
}

func (db *Database) compactLog() {
	// TODO: this could be more fine-grained
	db.l.Lock()
	defer db.l.Unlock()
	start := time.Now()
	defer db.Stats.AddTimeSince(start)
	updates := db.log.Updates()
	if len(updates) == 0 {
		return
	}
	t := db.mf.CreateTable()
	for _, e := range updates {
		t.Put(e)
	}
	table := t.Close()
	db.mf.InstallTable(table, nil, nil, 0)
	db.log.Close()
	db.fs.Truncate("log")
	db.log = initLog(db.fs)
}

func (db *Database) compactYoung() {
	db.l.RLock()
	start := time.Now()
	defer db.Stats.AddTimeSince(start)
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
	t := db.mf.CreateTable()
	it := MergeUpdates(updateIterators)
	for it.HasNext() {
		t.Put(it.Next())
	}
	table := t.Close()
	db.l.RUnlock()
	db.l.Lock()
	db.mf.InstallTable(table, youngTables, level1Tables, 1)
	db.l.Unlock()
}

// DeleteObsoleteFiles deletes files the database doesn't know about.
//
// This isn't currently correctly called by recovery, but at some point it is
// required to clean up partially constructed tables that weren't successfully
// added to the database.
func (db *Database) DeleteObsoleteFiles() {
	db.mf.cleanup()
}

// Compact manually triggers a full compaction of the log and tables.
func (db *Database) Compact() {
	db.compactLog()
	db.compactYoung()
}

// Close cleanly shuts down the database, and moreover pushes all data to tables
// for simple recovery.
func (db *Database) Close() {
	db.compactLog()
	db.log.Close()
}
