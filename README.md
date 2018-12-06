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
