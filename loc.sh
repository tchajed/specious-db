#!/bin/bash

# Get lines of code, ignoring test code and the filesystem implementation.

set -e

{ find . -name '*_test.go'; ls fs/afero_filesys.go; } > .ignored-files.txt

CLOC_ARGS="--quiet --hide-rate --by-file"
cloc $CLOC_ARGS --include-ext='go' --exclude-list-file=.ignored-files.txt . | tail +2

rm .ignored-files.txt
