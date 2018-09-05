package config

import (
	"sync"

	data "github.com/whiteboxio/flow/pkg/util/data"
)

type Registry struct {
	storage   *sync.Map
	providers map[string]Provider
}

var (
	registry = &Registry{
		storage:   &sync.Map{},
		providers: make(map[string]Provider),
	}
)

func Register(key string, prov Provider) error {
	keyHeap, _ := registry.storage.LoadOrStore(key, data.NewBinHeap())
	keyHeap.(*data.BinHeap).Insert(prov.GetWeight(), prov)
	registry.providers[prov.GetName()] = prov
	return nil
}

func Resolve() error {
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
	keyHeap, ok := registry.storage.Load(key)
	if !ok {
		return nil, ok
	}
	return keyHeap.(*data.BinHeap).GetMax().(Provider).GetValue(key)
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
		res[k.(string)], _ = v.(*data.BinHeap).GetMax().(Provider).GetValue(k.(string))
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
			top.Connect(dep, name)
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
