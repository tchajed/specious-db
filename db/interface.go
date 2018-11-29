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

type ErrKeyMissing struct{}

func (e ErrKeyMissing) Error() string {
	return "no such key"
}

type Store interface {
	Get(k Key) (Value, error)
	Put(k Key, v Value) error
	Delete(k Key) error
	// TODO: iterator API
}
