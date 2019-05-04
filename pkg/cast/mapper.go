package cast

import (
	"fmt"

	"github.com/awesome-flow/flow/pkg/types"
)

type Mapper interface {
	Map(kv *types.KeyValue) (*types.KeyValue, error)
}

type MapperNode struct {
	Mpr      Mapper
	Children map[string]*MapperNode
}

func NewMapperNode() *MapperNode {
	return &MapperNode{}
}

func (mn *MapperNode) Insert(key types.Key, mpr Mapper) *MapperNode {
	var ptr *MapperNode
	// Non-empty key check prevents users from accessing the root node
	if len(key) > 0 {
		ptr = mn
		for _, k := range key {
			if ptr.Children == nil {
				ptr.Children = make(map[string]*MapperNode)
			}
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
func (mn *MapperNode) Find(key types.Key) *MapperNode {
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

func (mn *MapperNode) DefineSchema(s Schema) error {
	return mn.doDefineSchema(types.NewKey(""), s)
}

func (mn *MapperNode) doDefineSchema(key types.Key, schema Schema) error {
	if mpr, ok := schema.(Mapper); ok {
		mn.Insert(key, mpr)
	} else if cnv, ok := schema.(Converter); ok {
		mn.Insert(key, NewConvMapper(cnv))
	} else if smap, ok := schema.(map[string]Schema); ok {
		if self, ok := smap["__self__"]; ok {
			// self: nil is used to emphasize an empty mapper for a federation structure
			if self != nil {
				if err := mn.doDefineSchema(key, self); err != nil {
					return err
				}
			}
		}
		for subKey, subSchema := range smap {
			if subKey == "__self__" {
				continue
			}
			if err := mn.doDefineSchema(append(key, subKey), subSchema); err != nil {
				return err
			}
		}
	} else {
		return fmt.Errorf("Unexpected schema definition type for key %q: %#v", key.String(), schema)
	}
	return nil
}

func (mn *MapperNode) Map(kv *types.KeyValue) (*types.KeyValue, error) {
	if ptr := mn.Find(kv.Key); ptr != nil && ptr.Mpr != nil {
		if mkv, err := ptr.Mpr.Map(kv); err != nil {
			return nil, err
		} else {
			return mkv, nil
		}
	}
	return kv, nil
}

type ConvMapper struct {
	conv Converter
}

var _ Mapper = (*ConvMapper)(nil)

func NewConvMapper(conv Converter) *ConvMapper {
	return &ConvMapper{conv}
}

func (cm *ConvMapper) Map(kv *types.KeyValue) (*types.KeyValue, error) {
	if mkv, ok := cm.conv.Convert(kv); ok {
		return mkv, nil
	}
	return nil, fmt.Errorf("Failed to convert value %#v for key %#v", kv.Key, kv.Value)
}
