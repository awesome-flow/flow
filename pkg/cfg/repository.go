package cfg

import (
	"sort"
	"sync"

	"github.com/awesome-flow/flow/pkg/cast"
	"github.com/awesome-flow/flow/pkg/util/data"
)

const (
	CfgPathKey    = "config.path"
	PluginPathKey = "plugins.path"
)

type Listener func(*cast.KeyValue)

type Provider interface {
	Name() string
	Depends() []string
	SetUp(*Repository) error
	TearDown(*Repository) error
	Get(cast.Key) (*cast.KeyValue, bool)
	Weight() int
}

var (
	mappers   *cast.MapperNode
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

func (n *node) add(key cast.Key, prov Provider) {
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

func (n *node) find(key cast.Key) *node {
	ptr := n
	for _, k := range key {
		if _, ok := ptr.children[k]; !ok {
			return nil
		}
		ptr = ptr.children[k]
	}
	return ptr
}

func (n *node) findOrCreate(key cast.Key) *node {
	ptr := n
	for _, k := range key {
		if _, ok := ptr.children[k]; !ok {
			ptr.children[k] = newNode()
		}
		ptr = ptr.children[k]
	}
	return ptr
}

func (n *node) subscribe(key cast.Key, listener Listener) {
	panic("not implemented")
}

func (n *node) get(repo *Repository, key cast.Key) (*cast.KeyValue, bool) {
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

func (n *node) getAll(repo *Repository, pref cast.Key) *cast.KeyValue {
	res := make(map[string]cast.Value)
	for k, ch := range n.children {
		key := cast.Key(append(pref, k))
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
	mkv, err := repo.doMap(&cast.KeyValue{pref, res})
	if err != nil {
		panic(err)
	}
	return mkv
}

type Repository struct {
	mappers   *cast.MapperNode
	root      *node
	providers map[string]Provider
	mx        sync.Mutex
}

func NewRepository() *Repository {
	return &Repository{
		mappers:   cast.NewMapperNode(),
		root:      newNode(),
		providers: make(map[string]Provider),
		mx:        sync.Mutex{},
	}
}

func (repo *Repository) SetUp() error {
	providers, err := repo.traverseProviders()
	if err != nil {
		return err
	}
	for _, prov := range providers {
		if err := prov.SetUp(repo); err != nil {
			return err
		}
	}

	return nil
}

func (repo *Repository) TearDown() error {
	repo.mx.Lock()
	defer repo.mx.Unlock()

	providers, err := repo.traverseProviders()
	if err != nil {
		return err
	}
	for _, prov := range providers {
		if err := prov.TearDown(repo); err != nil {
			return err
		}
	}
	return nil
}

func (repo *Repository) traverseProviders() ([]Provider, error) {
	provList := make([]data.TopologyNode, 0, len(repo.providers))
	for _, prov := range repo.providers {
		provList = append(provList, prov)
	}
	top := data.NewTopology(provList...)
	for name, prov := range repo.providers {
		for _, dep := range prov.Depends() {
			top.Connect(repo.providers[name], repo.providers[dep])
		}
	}
	resolved, err := top.Sort()
	if err != nil {
		return []Provider{}, err
	}
	res := make([]Provider, len(resolved))
	for ix, prov := range resolved {
		res[ix] = prov.(Provider)
	}
	return res, nil
}

func (repo *Repository) DefineSchema(s cast.Schema) error {
	return repo.mappers.DefineSchema(s)
}

func (repo *Repository) doMap(kv *cast.KeyValue) (*cast.KeyValue, error) {
	return repo.mappers.Map(kv)
}

func (repo *Repository) RegisterProvider(prov Provider) {
	repo.mx.Lock()
	defer repo.mx.Unlock()
	repo.providers[prov.Name()] = prov
}

//TODO: rename to RegisterKey
func (repo *Repository) Register(key cast.Key, prov Provider) {
	repo.mx.Lock()
	defer repo.mx.Unlock()
	repo.root.add(key, prov)
	if _, ok := repo.providers[prov.Name()]; !ok {
		repo.providers[prov.Name()] = prov
	}
}

//func (repo *Repository) Subscribe(key cast.Key, listener Listener) {
//	repo.root.subscribe(key, listener)
//}

func (repo *Repository) Get(key cast.Key) (cast.Value, bool) {
	// Non-empty key check prevents users from accessing a protected
	// root node
	if len(key) != 0 {
		if kv, ok := repo.root.get(repo, key); ok {
			return kv.Value, ok
		}
	}
	return nil, false
}
