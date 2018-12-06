#!/bin/bash

# Helper script to run benchmarks with a reasonable default configuration.

set -e

info() {
  echo -e "\033[1;37m${1}\033[0m"
}

die() {
  echo "$1" 1>&2
  exit 1
}

main() {
  DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null && pwd )"
  if [ -z "$1" ]; then
    die "Usage: bench.sh <path to specious-bench> <args for specious-bench>"
  fi
  BENCH_BIN="$1"
  shift
  if ! [ -f "$BENCH_BIN" ] && ! which "$BENCH_BIN" &>/dev/null; then
    die "could not find executable $BENCH_BIN" 1>&2
  fi

  info "mem"
  "$BENCH_BIN" -db mem -benchmarks=fillrandom,readrandom "$@"

  echo
  info "specious"

  local benchmarks="fillseq,readseq,init,fillrandom,readrandom,\
fs-write,fs-read"
  "$BENCH_BIN" -db specious -final-compact -delete-db \
               -benchmarks="$benchmarks" "$@"

  echo
  info "leveldb"
  local benchmarks="fillseq,readseq,init,fillrandom,readrandom"
  "$BENCH_BIN" -db leveldb -final-compact -delete-db \
               -benchmarks="$benchmarks" "$@"
}

main "$@"
