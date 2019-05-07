package cast

import (
	"fmt"

	"github.com/awesome-flow/flow/pkg/types"
)

// Mapper is a generic interface for mapping actors. These co-exist hand-by-hand
// with Converters. Converters are trivial convert-or-give-up actors, designed
// to act as a part of a more complex conversion/mapping logic. Mappers are not
// supposed to do conversion but mapping: produce complex structures from the
// input maps.
type Mapper interface {
	Map(kv *types.KeyValue) (*types.KeyValue, error)
}

// MapperNode is a data structure representing a trie node holding a Mapper and
// trie structure children.
type MapperNode struct {
	Mpr      Mapper
	Children map[string]*MapperNode
}

// NewMapperNode is the constructor for MapperNode.
func NewMapperNode() *MapperNode {
	return &MapperNode{}
}

// Insert effectively places the Mapper under the specified Key in the trie
// structure. If the trie path does not exist, it creates the necessary nodes.
// Insert supports wildcards in the Key path. This effectively relaxes the Find
// operation strictness.
//
// Example: Insert(Key("foo.*.baz"), m) means: a Find(key) lookup should follow
// path like: foo -> <any key> -> baz. Both foo.bar.baz and foo.moo.baz will
// match the search and return m.
// Wildcards have priority: a star match has a lower precedence than the exact
// match.
//
// Example:
//   Insert(Key("foo.*.baz"), m1)
//   Insert(Key("boo.bar.baz"), m2)
// In this case Find(Key("foo.moo.baz")) returns m2, whereas
// Find(Key("foo.bar.baz")) returns m1 because it's an exact match.
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

// Find performs a lookup of a relevant MapperNode in the trie structure by
// following the provided Key path. If the needle node could not be found,
// returns nil.
// Find supports wildcards. See `Insert()` for more details.
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

// DefineSchema is the primary way to bulk-register mappers in a MapperNode.
// Schema is a very flexible structure. See Schema docs for more details.
// If Schema is defined as a map[string]Schema, MapperNode will explicitly look
// up for a key called __self__ which is the way to provide a Mapper for the
// original node that also encorporates parent-children relationships.
//
// Example: a flat Schema might be defined like:
// schema := map[string]Schema{"foo": FooMapper}
//
// It's also possible to provide a Converter instead of a Mapper, in this case
// it would be automatically converted to a Mapper:
// schema := map[string]Schema{"foo": FooConverter}
//
// If `foo` is a parent node to a set of other keys and there is a composite
// mapper for `foo` itself, it might be achieved this way:
// schema := map[string]Schema{"foo": map[string]Schema{"__self__": FooMapper, "bar": BarMapper, "moo": MooConverter}}
// In this case a `foo` conversion lookup would be resolved this way:
// 1. Convert `moo` using MooMapper, convert `bar` using BarMapper.
// 2. Convert `foo` using FooMapper providing map[string]Value{"moo": MooVal, "bar": BarVal}
// 3. Return result.
//
// __self__ might be set to nil in the schema definition in order to emphasise
// an absence of the mapper for the parental key. It's fully equivalent to
// no-definition for key __self__.
func (mn *MapperNode) DefineSchema(s Schema) error {
	return mn.doDefineSchema(types.NewKey(""), s)
}

func (mn *MapperNode) doDefineSchema(key types.Key, schema Schema) error {
	if schema == nil {
		return nil
	} else if mpr, ok := schema.(Mapper); ok {
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

// Map performs the actual mapping of the key-value pair.
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

// ConvMapper is a helper wrapper that turns a single Converter into a Mapper
// structure with the expected bahavior: if Converter fails to convert, the
// wrapper Mapper returns an error.
type ConvMapper struct {
	conv Converter
}

var _ Mapper = (*ConvMapper)(nil)

// NewConvMapper is the constructor for ConvMapper.
func NewConvMapper(conv Converter) *ConvMapper {
	return &ConvMapper{conv}
}

// Map returns a key-value pair if the Converter recognised the value.
// Returns nil, err otherwise.
func (cm *ConvMapper) Map(kv *types.KeyValue) (*types.KeyValue, error) {
	if mkv, ok := cm.conv.Convert(kv); ok {
		return mkv, nil
	}
	return nil, fmt.Errorf("Failed to convert value %#v for key %#v", kv.Key, kv.Value)
}
