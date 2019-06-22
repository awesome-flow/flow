package cfg

import (
	"sort"
	"sync"

	"github.com/awesome-flow/flow/pkg/cast"
	"github.com/awesome-flow/flow/pkg/types"
	"github.com/awesome-flow/flow/pkg/util/data"
)

const (
	// CfgPathKey is a string constant used globally to reach up the config
	// file path setting.
	CfgPathKey = "config.path"
	// PluginPathKey is a string constant used globally to reach up the plugin
	// path setting.
	PluginPathKey = "plugin.path"
)

// TODO(olegs): implement listener interface
// type Listener func(*types.KeyValue)

// Provider is a generic interface for config providers.
// A method initializing a new instance of Provider must conform to Constructor
// type.
type Provider interface {
	Name() string
	Depends() []string
	SetUp(*Repository) error
	TearDown(*Repository) error
	Get(types.Key) (*types.KeyValue, bool)
	Weight() int
}

var (
	mappers   *cast.MapperNode
	mappersMx sync.Mutex
)

// Constructor is the signature Provider instances are expected to implement
// as a producing function.
type Constructor func(*Repository, int) (Provider, error)

type node struct {
	providers []Provider
	//listeners []Listener
	children map[string]*node
}

func newNode() *node {
	return &node{
		providers: make([]Provider, 0),
		//listeners: make([]Listener, 0),
		children: make(map[string]*node),
	}
}

func (n *node) explain(key types.Key) map[string]interface{} {
	res := map[string]interface{}{}
	if len(n.providers) > 0 {
		valdescr := make([]map[string]interface{}, 0, len(n.providers))
		for _, prov := range n.providers {
			if kv, ok := prov.Get(key); ok {
				valdescr = append(valdescr, map[string]interface{}{
					"provider_name":   prov.Name(),
					"provider_weight": prov.Weight(),
					"value":           kv.Value,
				})
			}
		}
		res["__value__"] = valdescr
	} else if len(n.children) > 0 {
		for k, ch := range n.children {
			res[k] = ch.explain(append(key, k))
		}
	}
	return res
}

func (n *node) add(key types.Key, prov Provider) {
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

func (n *node) find(key types.Key) *node {
	ptr := n
	for _, k := range key {
		if _, ok := ptr.children[k]; !ok {
			return nil
		}
		ptr = ptr.children[k]
	}
	return ptr
}

func (n *node) findOrCreate(key types.Key) *node {
	ptr := n
	for _, k := range key {
		if _, ok := ptr.children[k]; !ok {
			ptr.children[k] = newNode()
		}
		ptr = ptr.children[k]
	}
	return ptr
}

// func (n *node) subscribe(key types.Key, listener Listener) {
// 	panic("not implemented")
// }

func (n *node) get(repo *Repository, key types.Key) (*types.KeyValue, bool) {
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

func (n *node) getAll(repo *Repository, pref types.Key) *types.KeyValue {
	res := make(map[string]types.Value)
	for k, ch := range n.children {
		key := types.Key(append(pref, k))
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
	mkv, err := repo.doMap(&types.KeyValue{Key: pref, Value: res})
	if err != nil {
		panic(err)
	}
	return mkv
}

// Repository is a generic structure used by flow to store config maps and
// corresponding type mappers.
// There is 1 globally registered repository instance available by loading from
// global storage: `global.Load("config")`. It keeps the init-stage system
// settings and might be used by any consumer.
// Plugin code can instantiate and use locally defined repositories. Having
// independent repositories is practical.
type Repository struct {
	mappers   *cast.MapperNode
	root      *node
	providers map[string]Provider
	mx        sync.Mutex
}

// NewRepository returns a new instance of an empty Repository.
func NewRepository() *Repository {
	return &Repository{
		mappers:   cast.NewMapperNode(),
		root:      newNode(),
		providers: make(map[string]Provider),
		mx:        sync.Mutex{},
	}
}

// SetUp traverses registered providers and calls `provider.SetUp(repo)`.
// Providers are traversed in topological order, based on the dependencies
// they defined using `Depends()` method.
// Firstly, it sets up providers with no dependencies and progresses forward
// as providers with non-zero dependencies turn to be unblocked.
// Returns an error if at least 1 provider failed to call `SetUp`.
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

// TearDown does the opposite to `SetUp`: it prepares providers to get
// unloaded. The sequence of `provider.TearDown(repo)` is exactly the same
// as SetUp(): topologically sorted dependency list.
// Returns an error if at least 1 provider failed to call `TearDown`.
func (repo *Repository) TearDown() error {
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

// DefineSchema registers a schema in the repo.
// Multiple non-overlapping schemas might be registered sequentually with
// an equivalence of registering a composite schema at once.
// Returns an error if the root mapper node failes to register the schema.
func (repo *Repository) DefineSchema(s cast.Schema) error {
	return repo.mappers.DefineSchema(s)
}

func (repo *Repository) doMap(kv *types.KeyValue) (*types.KeyValue, error) {
	return repo.mappers.Map(kv)
}

// RegisterProvider marks a provider as known to the repository.
// A registered provider will be visited by `SetUp` and `TearDown` methods,
// but won't serve any key lookup requests yet. Used at the very early stage
// of the system initialization in order to trigger providers's `SetUp` method.
// This method is thread safe.
func (repo *Repository) RegisterProvider(prov Provider) {
	repo.mx.Lock()
	defer repo.mx.Unlock()
	repo.providers[prov.Name()] = prov
}

// RegisterKey registers a provider as a potential servant for the specified
// key.
// If a provider can serve multiple keys, every key registration must be
// created explicitly, 1 at a time.
// This method is thread safe.
func (repo *Repository) RegisterKey(key types.Key, prov Provider) {
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

// Get is the primary interface for the stored data retrieval.
// Returns the fetched value and a bool flag indicating the lookup result.
// If no value was retrived from the providers, bool flag is set to false.
func (repo *Repository) Get(key types.Key) (types.Value, bool) {
	// Non-empty key check prevents users from accessing a protected
	// root node
	if len(key) != 0 {
		if kv, ok := repo.root.get(repo, key); ok {
			return kv.Value, ok
		}
	}
	return nil, false
}

// Explain returns a structure with a detailed explanation of the repository.
// The resulting map mimics the original config map structure and leafs
// indicate per-provider breakdown with a corresponding value returned by
// each of them.
func (repo *Repository) Explain() map[string]interface{} {
	return repo.root.explain(nil)
}
