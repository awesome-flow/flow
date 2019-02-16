package config

import (
	"sort"
	"sync"

	data "github.com/awesome-flow/flow/pkg/util/data"
)

type Registry struct {
	storage   *sync.Map
	providers map[string]Provider
	lock      *sync.Mutex
}

var (
	registry = &Registry{
		storage:   &sync.Map{},
		providers: make(map[string]Provider),
		lock:      &sync.Mutex{},
	}
)

func Register(key string, prov Provider) error {
	registry.lock.Lock()
	defer registry.lock.Unlock()
	keyProvs, _ := registry.storage.LoadOrStore(key, make([]Provider, 0))
	v := append(keyProvs.([]Provider), prov)
	sort.Sort(sort.Reverse(ProviderList(v)))
	registry.storage.Store(key, v)
	registry.providers[prov.GetName()] = prov

	return nil
}

func Resolve() error {
	registry.lock.Lock()
	defer registry.lock.Unlock()

	registry.storage.Range(func(key interface{}, value interface{}) bool {
		sort.Sort(sort.Reverse(ProviderList(value.([]Provider))))
		return true
	})

	traversed, err := traverseProviders()
	if err != nil {
		return err
	}
	for _, prov := range traversed {
		if err := prov.Resolve(); err != nil {
			return err
		}
	}

	return nil
}

func Get(key string) (interface{}, bool) {
	provChain, ok := registry.storage.Load(key)
	if !ok {
		return nil, false
	}
	for _, prov := range provChain.([]Provider) {
		if v, ok := prov.GetValue(key); ok {
			return v, ok
		}
	}
	return nil, false
}

func GetOrDefault(key string, def interface{}) (interface{}, bool) {
	res, ok := Get(key)
	if !ok {
		return def, false
	}
	return res, true
}

func GetAll() map[string]interface{} {
	res := make(map[string]interface{})
	registry.storage.Range(func(k interface{}, v interface{}) bool {
		if v, ok := Get(k.(string)); ok {
			res[k.(string)] = v
		}
		return true
	})
	return res
}

func traverseProviders() ([]Provider, error) {
	provList := make([]data.TopologyNode, len(registry.providers))
	ix := 0
	for _, prov := range registry.providers {
		provList[ix] = prov
		ix++
	}
	top := data.NewTopology(provList...)
	for name, prov := range registry.providers {
		for _, dep := range prov.DependsOn() {
			top.Connect(registry.providers[name], registry.providers[dep])
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
