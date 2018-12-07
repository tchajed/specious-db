package db

// Maintains invariant that updates[i] is always the next value from
// iterators[i] (or nil iff the iterator is empty). This is established at
// initialization by MergeUpdates.
type mergedIterator struct {
	iterators []UpdateIterator
	updates   []*KeyUpdate
}

// advance attempts to store the next result of iterators[i] (or nil if the
// iterator is empty)
func (mi mergedIterator) advance(i int) {
	if mi.iterators[i].HasNext() {
		next := mi.iterators[i].Next()
		mi.updates[i] = &next
	} else {
		mi.updates[i] = nil
	}
}

// MergeUpdates takes several iterators and produces a merged iterator.
//
// iterators should be sorted by key, and MergeUpdates will produce an iterator
// that is also sorted.
func MergeUpdates(iterators []UpdateIterator) UpdateIterator {
	mi := mergedIterator{iterators, make([]*KeyUpdate, len(iterators))}
	for i := range iterators {
		mi.advance(i)
	}
	return mi
}

func (mi mergedIterator) HasNext() bool {
	for _, up := range mi.updates {
		if up != nil {
			return true
		}
	}
	return false
}

func (mi mergedIterator) Next() KeyUpdate {
	minIndex := 0
	var minUpdate *KeyUpdate
	for i, up := range mi.updates {
		if up == nil {
			continue
		}
		if minUpdate == nil || up.Key < minUpdate.Key {
			minIndex = i
			minUpdate = up
		}
	}
	mi.advance(minIndex)
	return *minUpdate
}
