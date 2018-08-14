package config

import (
	"fmt"
	"sync"

	data "github.com/whiteboxio/flow/pkg/data"
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
	for _, prov := range traverseProviders() {
		if err := prov.Resolve(); err != nil {
			return err
		}
	}
	return nil
}

func Unregister(key string, prov Provider) error {
	// TODO
	return fmt.Errorf("NOT IMPLEMENTED")
}

func Get(key string) (interface{}, bool) {
	keyHeap, ok := registry.storage.Load(key)
	if !ok {
		return nil, ok
	}
	return keyHeap.(*data.BinHeap).GetMax().(Provider).GetValue(key)
}

func traverseProviders() []Provider {
	tree := &data.NTree{}
	for provName, prov := range registry.providers {
		provNode := tree.FindOrInsert(provName)
		if depends := prov.DependsOn(); len(depends) > 0 {
			for _, depName := range depends {
				_, ok := registry.providers[depName]
				if !ok {
					panic(fmt.Sprintf("Provider %s is undefined but %s depends on it",
						depName, provName))
				}
				tree.Detach(depName)
				provNode.FindOrInsert(depName)
			}
		}
	}
	trvrsl := tree.PostTraversal()
	provs := make([]Provider, len(trvrsl))
	for ix, provName := range trvrsl {
		provs[ix] = registry.providers[provName.(string)]
	}
	return provs
}
