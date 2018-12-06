# Specious DB: a simple, persistent, key-value store

[![Build Status](https://travis-ci.org/tchajed/specious-db.svg?branch=master)](https://travis-ci.org/tchajed/specious-db)

A simple key-value store following the design of LevelDB, as a prototype of a verified key-value store.

We are aiming for the following basic design:
- crash safety using a write-ahead log
- transactional writes
- concurrent reads while writing to disk
- recovery
- background compaction

## Running benchmarks

Specious DB requires Go 1.9+; if you're running Ubuntu 18.04 or later, you can use `apt-get install golang-go`, otherwise you'll need a more recent version of Go.

Once you have Go, install leveldb:
- On Ubuntu, `apt-get install libleveldb-dev libleveldb1v5`
- On Arch Linux, `pacman -S leveldb`

Next, install into your $GOPATH with `go get -u github.com/tchajed/specious-db`.

Run the benchmarks with `$GOPATH/src/github.com/tchajed/specious-db/bench.sh $GOPATH/bin/specious-db`. From a clone of the repo you can more simply run `go build` followed by `./bench.sh ./specious-db`.

## Some benchmark numbers

On my Macbook (these are lines filtered from `bench.sh`):

```
           database: mem
fillseq         :   0.665 micros/op;  154.8 MB/s
readseq         :   0.114 micros/op;  906.5 MB/s
           database: specious
fillseq         :   6.315 micros/op;   16.3 MB/s
readseq         :   3.011 micros/op;   34.2 MB/s
fillrandom      :   6.289 micros/op;   16.4 MB/s
readrandom      :  68.097 micros/op;    1.5 MB/s
           database: leveldb
fillseq         :  10.392 micros/op;    9.9 MB/s
readseq         :   2.526 micros/op;   40.8 MB/s
fillrandom      :  10.487 micros/op;    9.8 MB/s
readrandom      :   4.500 micros/op;   22.9 MB/s
```

On galois (a Linux desktop with a fast SSD):

```
           database: mem
fillseq         :   0.820 micros/op;  125.6 MB/s
readseq         :   0.115 micros/op;  896.6 MB/s
           database: specious
fillseq         :   4.771 micros/op;   21.6 MB/s
readseq         :   2.098 micros/op;   49.1 MB/s
fillrandom      :   4.786 micros/op;   21.5 MB/s
readrandom      :  41.982 micros/op;    2.5 MB/s
           database: leveldb
fillseq         :   8.168 micros/op;   12.6 MB/s
readseq         :   2.878 micros/op;   35.8 MB/s
fillrandom      :   7.584 micros/op;   13.6 MB/s
readrandom      :   4.460 micros/op;   23.1 MB/s
```
