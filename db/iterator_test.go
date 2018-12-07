package db

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

type sliceUpdateIterator []KeyUpdate

func (it sliceUpdateIterator) HasNext() bool {
	return len(it) > 0
}

func (it *sliceUpdateIterator) Next() KeyUpdate {
	up := (*it)[0]
	*it = (*it)[1:]
	return up
}

func combineUpdates(data [][]KeyUpdate) (its []UpdateIterator) {
	for _, updates := range data {
		it := sliceUpdateIterator(updates)
		its = append(its, &it)
	}
	return
}

func expectedCombined(data [][]KeyUpdate) (expected []KeyUpdate) {
	for _, updates := range data {
		expected = append(expected, updates...)
	}
	sortUpdates(expected)
	return
}

func TestMergedIterator(t *testing.T) {
	assert := assert.New(t)
	for _, iteratorList := range [][][]KeyUpdate{
		{
			{putU(1, ""), putU(4, ""), putU(7, "")},
			{putU(2, ""), putU(10, "")},
			{putU(3, ""), putU(15, "")},
		},
		{
			{putU(1, ""), putU(4, ""), putU(7, "")},
			{},
			{putU(3, ""), putU(15, "")},
		},
		{
			{putU(1, ""), putU(2, "")},
			{},
			{putU(3, ""), putU(4, "")},
		},
		{
			{putU(1, ""), putU(2, "")},
		},
	} {
		expected := expectedCombined(iteratorList)
		it := MergeUpdates(combineUpdates(iteratorList))
		var actual []KeyUpdate
		for i := 0; it.HasNext(); i++ {
			if i > len(expected)+1 {
				assert.Fail("iterator advanced too many times")
				break
			}
			actual = append(actual, it.Next())
		}
		assert.Equal(expected, actual)
	}
}
