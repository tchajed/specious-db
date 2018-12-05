#!/bin/bash

# Helper script to run benchmarks with a reasonable default configuration.

info() {
  echo -e "\033[1;37m${1}\033[0m"
}

go build

info "noop"
./specious-db -db noop

echo
info "specious"
./specious-db -db specious -compact-every 100000 -final-compact true -delete-db

echo
info "leveldb"
./specious-db -db leveldb -compact-every 0 -final-compact true -delete-db
