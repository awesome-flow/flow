package types

import "strings"

const (
	// KeySepCh is a char used as a key fragment separator. A dot by default.
	KeySepCh = "."
)

// Key type represents a key used in key-value relationships. A key is a
// composite structure: itconsists of fragments. Say, a key `foo.bar.baz`
// consists of 3 fragments: []string{"foo", "bar", "baz"} (split by `KeySepCh`).
type Key []string

// String satisfies Stringer interface
func (key Key) String() string {
	return strings.Join(key, KeySepCh)
}

// NewKey is a default constructor used for a new key instantiation.
// Automatically splits the input string into key fragments.
func NewKey(str string) Key {
	if len(str) == 0 {
		return Key(nil)
	}
	return Key(strings.Split(str, KeySepCh))
}

// Value represents a value in key-value relationships.
type Value interface{}

// KeyValue represents a basic key-value pair. It's a composite data structure.
type KeyValue struct {
	Key   Key
	Value Value
}

// Params is a simple string-Value map, used to pass flattened parameters.
type Params map[string]Value
