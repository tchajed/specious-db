package db

type Key = uint64
type Value = []byte

type Entry struct {
	Key
	Value
}

type MaybeValue struct {
	Present bool
	Value
}

var NoValue = MaybeValue{Present: false, Value: nil}

func SomeValue(v Value) MaybeValue {
	return MaybeValue{Present: true, Value: v}
}

type Store interface {
	Get(k Key) MaybeValue
	Put(k Key, v Value)
	Delete(k Key)
	// TODO: iterator API
	Close()
}

type KeyRange struct {
	Min uint64
	Max uint64
}

func (r KeyRange) Contains(k Key) bool {
	return r.Min <= k && k <= r.Max
}
