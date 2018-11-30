package db

type Key = []byte
type Value = []byte

type Entry struct {
	Key
	Value
}

func KeyEq(k1 Key, k2 Key) bool {
	if len(k1) != len(k2) {
		return false
	}
	for i := range k1 {
		if k1[i] != k2[i] {
			return false
		}
	}
	return true
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
