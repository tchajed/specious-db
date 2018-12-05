#!/bin/bash

# Helper script to run benchmarks with a reasonable default configuration.

set -e

info() {
  echo -e "\033[1;37m${1}\033[0m"
}

main() {
  DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null && pwd )"
  if [ -n "$1" ]; then
    BENCH_BIN="$1"
    shift
    if ! [ -f "$BENCH_BIN" ] && ! which "$BENCH_BIN" >/dev/null; then
      echo "could not find executable $BENCH_BIN" 1>&2
      exit 1
    fi
  else
    old_dir="$PWD"
    cd "$DIR"
    go build
    cd "$old_dir"
    BENCH_BIN="${DIR}/specious-db"
  fi

  info "noop"
  "$BENCH_BIN" -db noop "$@"

  echo
  info "specious"
  "$BENCH_BIN" -db specious -final-compact -delete-db "$@"

  echo
  info "leveldb"
  "$BENCH_BIN" -db leveldb -final-compact -delete-db "$@"
}

main "$@"
