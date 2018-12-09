#!/bin/bash

# Get lines of code, ignoring test code and the filesystem implementation.

set -e

find . -name '*_test.go' > .ignored-files.txt

CLOC="cloc --quiet --hide-rate --by-file"
$CLOC --include-ext='go' --exclude-list-file=.ignored-files.txt db bin log fs/fs.go | tail -n +2

rm .ignored-files.txt
