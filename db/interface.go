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
	return r.Min <= k && k <= r.Max
}

// adapter for simpler usage
type StringStore struct{ Store Store }

func (s StringStore) Get(k int) string {
	v := s.Store.Get(Key(k))
	if v.Present {
		return string(v.Value)
	}
	return "<missing>"
}

func (s StringStore) Put(k int, v string) {
	s.Store.Put(Key(k), []byte(v))
}

func (s StringStore) Delete(k int) {
	s.Store.Delete(Key(k))
}
