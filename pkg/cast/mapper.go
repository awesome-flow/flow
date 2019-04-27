package cast

import "fmt"

type Mapper interface {
	Map(kv *KeyValue) (*KeyValue, error)
}

type MapperNode struct {
	Mpr      Mapper
	Children map[string]*MapperNode
}

func NewMapperNode() *MapperNode {
	return &MapperNode{nil, make(map[string]*MapperNode)}
}

func (mn *MapperNode) Insert(key Key, mpr Mapper) *MapperNode {
	var ptr *MapperNode
	// Non-empty key check prevents users from accessing the root node
	if len(key) > 0 {
		ptr = mn
		for _, k := range key {
			if _, ok := ptr.Children[k]; !ok {
				ptr.Children[k] = NewMapperNode()
			}
			ptr = ptr.Children[k]
		}
		ptr.Mpr = mpr
	}

	return ptr
}

// This search supports wildcards. Exact match has a higher precedence over the
// wildcarded node.
// Example: a.*.c Vs a.b.c: a.b.c wins for a.b.c lookup, but a.*.c is returned
// for a.f.c
func (mn *MapperNode) Find(key Key) *MapperNode {
	if len(key) == 0 {
		return mn
	}
	for _, nextK := range []string{key[0], "*"} {
		if next, ok := mn.Children[nextK]; ok {
			if res := next.Find(key[1:]); res != nil {
				return res
			}
		}
	}
	return nil
}

type ConvMapper struct {
	conv Converter
}

var _ Mapper = (*ConvMapper)(nil)

func NewConvMapper(conv Converter) *ConvMapper {
	return &ConvMapper{conv}
}

func (cm *ConvMapper) Map(kv *KeyValue) (*KeyValue, error) {
	if mkv, ok := cm.conv.Convert(kv); ok {
		return mkv, nil
	}
	return nil, fmt.Errorf("Failed to convert value %#v for key %#v", kv.Key, kv.Value)
}
