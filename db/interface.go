package db

type Key = uint64
type Value = []byte

type Entry struct {
	Key
	Value
}

func KeyEq(k1 Key, k2 Key) bool {
	return k1 == k2
}

type MaybeValue struct {
	Present bool
	Value
}

var NoValue = MaybeValue{Present: false, Value: nil}

func SomeValue(v Value) MaybeValue {
	return MaybeValue{Present: true, Value: v}
}

func (mv MaybeValue) OrElse(f func() MaybeValue) MaybeValue {
	if mv.Present {
		return mv
	} else {
		return f()
	}
}

// NOTE: this is a commonly used higher-order interface that will probably
// require a first order solution (and ideally a nice abstraction)
type EntryIterator interface {
	Next() Entry
	IsDone() bool
}

type Store interface {
	Get(k Key) MaybeValue
	Put(k Key, v Value)
	Delete(k Key)
	// TODO: iterator API
}

type KeyRange struct {
	Min uint64
	Max uint64
}

func (r KeyRange) Contains(k Key) bool {
	// TODO: make keys uint64 so this is just an easy comparison
	panic("not implemented")
}
