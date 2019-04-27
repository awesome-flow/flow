package cfg

import (
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

func init() {
	mappers = newMapperNode()
}

func DefineMapper(path string, mapper Mapper) error {
	mappersMx.Lock()
	defer mappersMx.Unlock()

	mappers.Insert(NewKey(path), mapper)

	return nil
}

func doMap(kv *KeyValue) (*KeyValue, error) {
	if mn := mappers.Find(kv.Key); mn != nil && mn.mpr != nil {
		if mkv, err := mn.mpr.Map(kv); err != nil {
			return nil, err
		} else {
			return mkv, nil
		}
	}
	return kv, nil
}

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
	ptr := n.findOrCreate(key)
	ptr.listeners = append(ptr.listeners, listener)
}

func (n *node) get(key Key) (*KeyValue, bool) {
	ptr := n.find(key)
	if ptr == nil {
		return nil, false
	}
	if len(ptr.providers) != 0 {
		for _, prov := range ptr.providers {
			if kv, ok := prov.Get(key); ok {
				if mkv, err := doMap(kv); err != nil {
					panic(err)
				} else {
					return mkv, ok
				}
			}
		}
		return nil, false
	}
	if len(ptr.children) != 0 {
		return ptr.getAll(key), true
	}
	return nil, false
}

func (n *node) getAll(pref Key) *KeyValue {
	res := make(map[string]Value)
	for k, ch := range n.children {
		key := Key(append(pref, k))
		if len(ch.providers) > 0 {
			// Providers are expected to be sorted
			for _, prov := range ch.providers {
				if kv, ok := prov.Get(key); ok {
					mkv, err := doMap(kv)
					if err != nil {
						panic(err)
					}
					res[k] = mkv.Value
					break
				}
			}
		} else {
			res[k] = ch.getAll(key).Value
		}
	}
	mkv, err := doMap(&KeyValue{pref, res})
	if err != nil {
		panic(err)
	}
	return mkv
}

type Repository struct {
	mappers map[string]Mapper
	root    *node
}

func NewRepository() *Repository {
	return &Repository{
		root: newNode(),
	}
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
		if kv, ok := repo.root.get(key); ok {
			return kv.Value, ok
		}
	}
	return nil, false
}
