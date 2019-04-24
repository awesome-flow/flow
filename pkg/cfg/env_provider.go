package cfg

import (
	"os"
	"strings"
)

var blacklist map[string]bool

func init() {
	blacklist = map[string]bool{
		//TODO(olegs): fill it in
	}
}

type EnvProvider struct {
	weight   int
	registry map[string]Value
	ready    chan struct{}
}

var _ Provider = (*EnvProvider)(nil)

func NewEnvProvider(repo *Repository, weight int) (*EnvProvider, error) {
	return &EnvProvider{
		weight: weight,
		ready:  make(chan struct{}),
	}, nil
}

func (ep *EnvProvider) Name() string      { return "env" }
func (ep *EnvProvider) Depends() []string { return []string{"default"} }
func (ep *EnvProvider) Weight() int       { return ep.weight }

func (ep *EnvProvider) SetUp(repo *Repository) error {
	registry := make(map[string]Value)
	var k string
	var v interface{}

	for _, kv := range os.Environ() {
		if strings.HasPrefix(kv, "_") {
			continue
		}
		if ix := strings.Index(kv, "="); ix != -1 {
			k, v = kv[:ix], kv[ix+1:]
		} else {
			k, v = kv, true
		}
		k = canonise(k)
		if blacklist[k] {
			continue
		}
		registry[k] = v
		if repo != nil {
			repo.Register(NewKey(k), ep)
		}
	}

	ep.registry = registry
	close(ep.ready)

	return nil
}

func (ep *EnvProvider) TearDown(_ *Repository) error { return nil }

func (ep *EnvProvider) Get(key Key) (*KeyValue, bool) {
	<-ep.ready
	if val, ok := ep.registry[key.String()]; ok {
		return &KeyValue{key, val}, ok
	}
	return nil, false
}

func canonise(key string) string {
	return strings.ToLower(strings.Replace(key, "_", ".", -1))
}
