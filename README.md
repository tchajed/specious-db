# Specious DB: a simple, persistent, key-value store

[![Build Status](https://travis-ci.org/tchajed/specious-db.svg?branch=master)](https://travis-ci.org/tchajed/specious-db)

A simple key-value store following the design of LevelDB, as a prototype of a verified key-value store.

We are aiming for the following basic design:
- crash safety using a write-ahead log
- transactional writes
- concurrent reads while writing to disk
- recovery
- background compaction

See [design.md](design.md) for a more detailed overview of how the system works.

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
