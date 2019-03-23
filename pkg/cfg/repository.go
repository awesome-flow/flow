package cfg

import (
	"sort"
	"strings"
)

type node struct {
	branches map[string]*node
	agents   []Agent
}

func newNode() *node {
	return &node{
		branches: make(map[string]*node),
		agents:   make([]Agent, 0),
	}
}

type Repository struct {
	root *node
}

type Agent interface {
	Weight() int
	SetUp() error
	Values(Key) chan Value
}

type Key []string
type Value interface{}

func NewKey(k string) Key {
	return Key(strings.Split(k, "."))
}

func (k Key) String() string {
	return strings.Join(k, ".")
}

func (repo *Repository) Get(key Key) (Value, bool) {
	ptr := repo.lookup(key)
	if ptr == nil {
		return nil, false
	}
	if len(ptr.agents) == 0 {
		return nil, false
	}
	v := <-ptr.agents[0].Values(key)
	return v, true
}

func (repo *Repository) GetAll(key Key) map[string]Value {
	res := make(map[string]Value)
	ptr := repo.lookup(key)
	if ptr == nil {
		return nil
	}
	var traverse func(Key, *node)
	traverse = func(key Key, ptr *node) {
		if ptr == nil {
			return
		}
		if len(ptr.agents) != 0 {
			v := <-ptr.agents[0].Values(key)
			res[key.String()] = v
		}
		for suff, br := range ptr.branches {
			traverse(append(key, suff), br)
		}
	}
	traverse(key, ptr)
	return res
}

func (repo *Repository) lookup(key Key) *node {
	ptr := repo.root
	for _, k := range key {
		if ptr == nil {
			break
		}
		ptr = ptr.branches[k]
	}
	if ptr == nil {
		return nil
	}

	return ptr
}

//func (repo *Repository) Subscribe(key Key) (chan Value, bool) {
//	ch := make(chan)
//}

func (repo *Repository) Register(key Key, agent Agent) {
	ptr := repo.root
	for _, k := range key {
		if _, ok := ptr.branches[k]; !ok {
			ptr.branches[k] = newNode()
		}
		ptr = ptr.branches[k]
	}
	ptr.agents = append(ptr.agents, agent)
	sort.Slice(ptr.agents, func(i, j int) bool {
		return ptr.agents[i].Weight() < ptr.agents[j].Weight()
	})
}
