# Specious DB: a simple, persistent, key-value store

[![Build Status](https://travis-ci.org/tchajed/specious-db.svg?branch=master)](https://travis-ci.org/tchajed/specious-db)

A simple key-value store following the design of LevelDB, as a prototype of a verified key-value store.

We are aiming for the following basic design:
- crash safety using a write-ahead log
- transactional writes
- concurrent reads while writing to disk
- recovery
- background compaction

See below for more details on the design.

## Running benchmarks

Specious DB supports Go 1.8+; if you're running Ubuntu 18.04, you should be able use `apt-get install golang-go` (for 16.04 you'll need a more up-to-date version of Go). Performance is likely to be better on more recent versions, so it's best to use Go 1.10 or 1.11.

Once you have Go, install leveldb:
- On Ubuntu, `apt-get install libleveldb-dev libleveldb1v5`
- On Arch Linux, `pacman -S leveldb`

To run the benchmarks by installing the benchmark runner, install `specious-bench` with `go get -u github.com/tchajed/specious-db/cmd/specious-bench`.
Run the benchmarks with `$GOPATH/src/github.com/tchajed/specious-db/bench.sh $GOPATH/bin/specious-bench`.

To instead run from the repo:
- fetch the dependencies with `go get ./...`
- compile the benchmarking executable `go build ./cmd/specious-bench`
- run a set of benchmarks with `./bench.sh ./specious-bench`

## Some benchmark numbers

On my Macbook (these are lines filtered from `bench.sh`):

```
           database: mem
fillrandom           :   0.572 micros/op;  180.0 MB/s
readrandom           :   0.248 micros/op;  415.6 MB/s
           database: specious
fillseq              :   8.661 micros/op;   11.9 MB/s
readseq              :   2.402 micros/op;   42.9 MB/s
fillrandom           :   8.648 micros/op;   11.9 MB/s
readrandom           :   4.894 micros/op;   21.0 MB/s
fs-write             :  14.517 micros/op;  269.1 MB/s
fs-read              :   2.482 micros/op; 1573.5 MB/s
           database: leveldb
fillseq              :   9.993 micros/op;   10.3 MB/s
readseq              :   2.554 micros/op;   40.3 MB/s
fillrandom           :   9.970 micros/op;   10.3 MB/s
readrandom           :   4.039 micros/op;   25.5 MB/s
```

On galois (a Linux desktop with a fast SSD):

```
           database: mem
fillrandom           :   0.662 micros/op;  155.6 MB/s
readrandom           :   0.223 micros/op;  461.7 MB/s
           database: specious
fillseq              :   7.668 micros/op;   13.4 MB/s
readseq              :   1.651 micros/op;   62.4 MB/s
fillrandom           :   7.692 micros/op;   13.4 MB/s
readrandom           :   3.275 micros/op;   31.5 MB/s
fs-write             :   4.435 micros/op;  880.7 MB/s
fs-read              :   1.882 micros/op; 2074.7 MB/s
           database: leveldb
fillseq              :   8.424 micros/op;   12.2 MB/s
readseq              :   2.825 micros/op;   36.5 MB/s
fillrandom           :   7.532 micros/op;   13.7 MB/s
readrandom           :   4.514 micros/op;   22.8 MB/s

```

# System design overview

## Interface

Specious DB is a persistent key-value store. It supports puts, gets, and deletes.

The design closely follows LevelDB, in particular with a write-ahead log for crash safety of updates and periodic compaction to coalesce repeated updates (saving space) as well as to switch to a sorted format for faster reads.

Specious has the following limitations compared to LevelDB:
- Keys are fixed size and 64 bits (8 bytes).
- Values are limited to 65535 bytes.
- No support for snapshots (which are ephemeral iterators in LevelDB).
- No compression or caching.
- (currently) Compactions block the entire database.
- (currently) No concurrency; clients must issue one operation at a time.

## Log-structured merge trees

Specious uses a log-structured merge tree (LSM). Writes are first logged in a write-ahead log for crash safety. The log is append-only for efficient writes; to read data in the log, the database keeps an in-memory cache in a hashmap for fast reads. Eventually the log fills up and is converted to an immutable table with keys in sorted order (LevelDB calls this an SSTable). The table has an index with key ranges and pointers into the table for updates for those keys. Every table's index is cached, but not the data. To find a key, the database only needs to consider entries that contain the key, reading all of the updates and searching within this small range. Furthermore, the entire table is sorted so the index entry key ranges can be binary searched.

These tables may overlap, which means reads need to consider multiple tables. To solve this problem,
table are organized into a hierarchy of levels (currently specious only supports two levels) and data is moved from lower levels to higher levels. To move data the database compacts multiple tables at L(k) into a single larger table at a L(k+1). The young level, L0, is special because it is the only level where tables may overlap; the database ensures that L(k) for k > 0 (currently just L1)  has non-overlapping tables so that reads only need to search a single table.

One way to understand the structure of the database is to consider the entire read path. First, reads must consult the write-ahead log; these writes supersede older data in the tables. As a consequence, deletes are stored in the log to shadow earlier puts. Next, reads search the young level. Recall that the young level is special because its tables have overlapping key ranges. The tables in the young level are aged from older to newer, and reads must consult newer tables first so that later updates can overwrite older ones (including deletes, which need to be stored in the young level to mask puts in old young tables). Finally, if a key is not found in the log or young level the database searches each level from L(k) to the top. Each level has disjoint tables, so this only involves a single table search.

When the database performs a compaction, it takes several tables and constructs a new representation of the same data. Tables are immutable, except that compaction can copy the writes from an immutable to a new table and then safely delete the old table. Compaction at the young level is a bit trickier because the tables are ordered and because tables can overlap. For correctness, the database must compact a prefix of young tables, and to maintain disjointness of L1 it should also included all overlapping L1 tables in the same compaction. For L1 and higher compactions can take any set of tables at L(k) and all the overlapping tables at L(k+1) and compact them to a table in L(k+1).

# Abstraction layers

## File system

Restricted filesystem interface for a single directory:

- state: map from names (strings) to files (byte arrays)
- operations:
  - reading fixed ranges, reading entire files, appending to files, deletes
  - does not support renames
  - has an atomic create API which internally uses rename, only used to manage the manifest

## Log

Supports transactional writes of bytes. Callers can `Add` transactions (byte sequences), and upon crash can recover a list of committed transactions. After safely processing the log, the caller can truncate the log (the log format makes this an `ftruncate` by ensuring an empty log file is an empty log); because of crashes during recovery, the caller's processing of the transactions needs to be idempotent. Implemented as a sequence of (data record, commit record) pairs. Crash safety relies on commit records being written atomically (easy, they're one byte) and after the data. This can be guaranteed in the following ways:

  - `fdatasync` after writing the data
  - issue a (non-existent) ordering call to the filesystem
  - checksum the log
  - assume filesystem appends persist in order (even byte-by-byte), and then promise a prefix of transactions is on disk
  - assume filesystem writes are immediately persistent (that is, the computer never crashes, only the proceses)

Note that this log only handles the transaction part, guaranteeing transactions are atomic and in order; higher levels interpret the transactions in the log as operations of some sort (eg, the database stores key-value updates, and a filesystem implementation might use this API to store block writes).

## Tables

(implemented using filesystem)

Tables are immutable and stored on disk in sorted order. They have an index stored on disk and cached in memory for efficient reads. When created, they support streaming updates to disk (the caller is responsible for doing so in order), and afterward support efficient reads using a binary search over the index ranges and then a linear scan within each index entry set of updates. During recovery, tables are opened from disk, which reads the on-disk index.

## Database write-ahead log

(implemented using log)

The database uses the log layer to record updates. Each transaction is a sequence of updates (puts or deletes). The database log also includes a caching layer that stores updates in a hashtable for reads and iteration (which is required for writing out the log to a table). On recovery, the database compacts any writes in the write-ahead log to a table and then truncates the log. This is safe because constructing the table is idempotent; duplicate tables are safe, though inefficient in terms of storage until they are compacted together.

## Manifest

(implemented using filesystem and tables, but could be ported to use an instance of the log and tables)

The manifest tracks a set of tables, including holding references to all the open tables (which is all of them). It supports creating a new table atomically with deleting old tables, with a similar streaming API. It also forwards reads to the appropriate table, implementing the tiered search over the levels (in reverse chronological order for L0). The manifest also keeps track of metadata in a crash safe manner; currently this is implemented by atomically writing out a new representation for every change. On recovery the manifest records what tables are in the database and what level each table is at.

## Database

(implemented using write-ahead log and manifest)

The database manages a write-ahead log and the manifest. Writes go to the log, reads start with the log and then search the manifest, log compaction takes data from the log and writes it to a new L0 table, and level compaction takes tables and writes out new tables (taking care to incorporate enough tables for correctness).
