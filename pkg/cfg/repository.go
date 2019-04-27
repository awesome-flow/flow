package cfg

import (
	"fmt"
	"sort"
	"strings"
	"sync"
)

const (
	KeySepCh      = "."
	CfgPathKey    = "config.path"
	PluginPathKey = "plugins.path"
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

type Listener func(*KeyValue)

type Provider interface {
	Name() string
	Depends() []string
	SetUp(*Repository) error
	TearDown(*Repository) error
	Get(Key) (*KeyValue, bool)
	Weight() int
}

var (
	mappers   *mapperNode
	mappersMx sync.Mutex
)

type Constructor func(*Repository, int) (Provider, error)

type node struct {
	providers []Provider
	listeners []Listener
	children  map[string]*node
}

func newNode() *node {
	return &node{
		providers: make([]Provider, 0),
		listeners: make([]Listener, 0),
		children:  make(map[string]*node),
	}
}

func (n *node) add(key Key, prov Provider) {
	ptr := n
	for _, k := range key {
		if _, ok := ptr.children[k]; !ok {
			ptr.children[k] = newNode()
		}
		ptr = ptr.children[k]
	}
	ptr.providers = append(ptr.providers, prov)
	sort.Slice(ptr.providers, func(a, b int) bool {
		return ptr.providers[a].Weight() > ptr.providers[b].Weight()
	})
}

func (n *node) find(key Key) *node {
	ptr := n
	for _, k := range key {
		if _, ok := ptr.children[k]; !ok {
			return nil
		}
		ptr = ptr.children[k]
	}
	return ptr
}

func (n *node) findOrCreate(key Key) *node {
	ptr := n
	for _, k := range key {
		if _, ok := ptr.children[k]; !ok {
			ptr.children[k] = newNode()
		}
		ptr = ptr.children[k]
	}
	return ptr
}

func (n *node) subscribe(key Key, listener Listener) {
	panic("not implemented")
}

func (n *node) get(repo *Repository, key Key) (*KeyValue, bool) {
	ptr := n.find(key)
	if ptr == nil {
		return nil, false
	}
	if len(ptr.providers) != 0 {
		for _, prov := range ptr.providers {
			if kv, ok := prov.Get(key); ok {
				if mkv, err := repo.doMap(kv); err != nil {
					panic(err)
				} else {
					return mkv, ok
				}
			}
		}
		return nil, false
	}
	if len(ptr.children) != 0 {
		return ptr.getAll(repo, key), true
	}
	return nil, false
}

func (n *node) getAll(repo *Repository, pref Key) *KeyValue {
	res := make(map[string]Value)
	for k, ch := range n.children {
		key := Key(append(pref, k))
		if len(ch.providers) > 0 {
			// Providers are expected to be sorted
			for _, prov := range ch.providers {
				if kv, ok := prov.Get(key); ok {
					mkv, err := repo.doMap(kv)
					if err != nil {
						panic(err)
					}
					res[k] = mkv.Value
					break
				}
			}
		} else {
			res[k] = ch.getAll(repo, key).Value
		}
	}
	mkv, err := repo.doMap(&KeyValue{pref, res})
	if err != nil {
		panic(err)
	}
	return mkv
}

type Repository struct {
	mappers *mapperNode
	root    *node
}

func NewRepository() *Repository {
	return &Repository{
		mappers: newMapperNode(),
		root:    newNode(),
	}
}

type Schema interface{}

func (repo *Repository) DefineSchema(s Schema) error {
	return repo.doDefineSchema(NewKey(""), s)
}

func (repo *Repository) doDefineSchema(key Key, schema Schema) error {
	if mpr, ok := schema.(Mapper); ok {
		repo.mappers.Insert(key, mpr)
	} else if cnv, ok := schema.(Converter); ok {
		repo.mappers.Insert(key, NewConvMapper(cnv))
	} else if smap, ok := schema.(map[string]Schema); ok {
		if self, ok := smap["__self__"]; ok {
			// self: nil is used to emphasize an empty mapper for a federation structure
			if self != nil {
				if err := repo.doDefineSchema(key, self); err != nil {
					return err
				}
			}
		}
		for subKey, subSchema := range smap {
			if subKey == "__self__" {
				continue
			}
			if err := repo.doDefineSchema(append(key, subKey), subSchema); err != nil {
				return err
			}
		}
	} else {
		return fmt.Errorf("Unexpected schema definition type for key %q: %#v", key.String(), schema)
	}
	return nil
}

func (repo *Repository) doMap(kv *KeyValue) (*KeyValue, error) {
	if mn := repo.mappers.Find(kv.Key); mn != nil && mn.mpr != nil {
		if mkv, err := mn.mpr.Map(kv); err != nil {
			return nil, err
		} else {
			return mkv, nil
		}
	}
	return kv, nil
}

func (repo *Repository) Register(key Key, prov Provider) {
	repo.root.add(key, prov)
}

func (repo *Repository) Subscribe(key Key, listener Listener) {
	repo.root.subscribe(key, listener)
}

func (repo *Repository) Get(key Key) (Value, bool) {
	// Non-empty key check prevents users from accessing a protected
	// root node
	if len(key) != 0 {
		if kv, ok := repo.root.get(repo, key); ok {
			return kv.Value, ok
		}
	}
	return nil, false
}
