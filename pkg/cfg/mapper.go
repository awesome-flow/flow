package cfg

type Mapper interface {
	Map(kv *KeyValue) (*KeyValue, error)
}

type mapperNode struct {
	mpr      Mapper
	children map[string]*mapperNode
}

func newMapperNode() *mapperNode {
	return &mapperNode{nil, make(map[string]*mapperNode)}
}

func (mn *mapperNode) Insert(key Key, mpr Mapper) *mapperNode {
	var ptr *mapperNode
	// Non-empty key check prevents users from accessing the root node
	if len(key) > 0 {
		ptr = mn
		for _, k := range key {
			if _, ok := ptr.children[k]; !ok {
				ptr.children[k] = newMapperNode()
			}
			ptr = ptr.children[k]
		}
		ptr.mpr = mpr
	}

	return ptr
}

// This search supports wildcards. Exact match has a higher precedence over the
// wildcarded node.
// Example: a.*.c Vs a.b.c: a.b.c wins for a.b.c lookup, but a.*.c is returned
// for a.f.c
func (mn *mapperNode) Find(key Key) *mapperNode {
	if len(key) == 0 {
		return mn
	}
	for _, nextK := range []string{key[0], "*"} {
		if next, ok := mn.children[nextK]; ok {
			if res := next.Find(key[1:]); res != nil {
				return res
			}
		}
	}
	return nil
}
