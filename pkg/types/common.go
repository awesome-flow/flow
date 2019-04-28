package types

import "strings"

const (
	KeySepCh = "."
)

type Key []string

func (key Key) String() string {
	return strings.Join(key, KeySepCh)
}

func NewKey(str string) Key {
	if len(str) == 0 {
		return Key(nil)
	}
	return Key(strings.Split(str, KeySepCh))
}

type Value interface{}

type KeyValue struct {
	Key   Key
	Value Value
}

type Params map[string]Value
