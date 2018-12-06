package db

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func linearSearch(entries []indexEntry, k Key) int {
	for i, e := range entries {
		if e.Keys.Contains(k) {
			return i
		}
	}
	return -1
}

func entry(min Key, max Key) indexEntry {
	return indexEntry{Keys: KeyRange{min, max}}
}

type testCase struct {
	entries    []indexEntry
	keysToTest []Key
}

func (test testCase) prettyEntries() string {
	var entries []string
	for i, e := range test.entries {
		entries = append(entries, fmt.Sprintf("%d: (%d %d)", i, e.Keys.Min, e.Keys.Max))
	}
	return fmt.Sprintf("%v", entries)
}

func TestBinSearch(t *testing.T) {
	tests := []testCase{
		{[]indexEntry{entry(0, 3), entry(5, 6), entry(10, 11), entry(12, 12), entry(14, 25)},
			[]Key{0, 1, 4, 12, 14, 17, 25}},
		{[]indexEntry{entry(5, 20)},
			[]Key{0, 5, 10, 20, 25}},
		{[]indexEntry{entry(1, 1), entry(2, 2), entry(10, 10)},
			[]Key{1, 2, 10}},
	}
	for _, test := range tests {
		for _, k := range test.keysToTest {
			assert.Equal(t, linearSearch(test.entries, k), binSearch(test.entries, k),
				"search for key %d in %s", k, test.prettyEntries())
		}
	}
}
