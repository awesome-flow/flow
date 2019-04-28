package cfg

import (
	"os"
	"strings"

	"github.com/awesome-flow/flow/pkg/types"
)

var blacklist map[string]bool

func init() {
	blacklist = map[string]bool{
		//TODO(olegs): fill it in
	}
}

type EnvProvider struct {
	weight   int
	registry map[string]types.Value
	ready    chan struct{}
}

var _ Provider = (*EnvProvider)(nil)

func NewEnvProvider(repo *Repository, weight int) (*EnvProvider, error) {
	prov := &EnvProvider{
		weight: weight,
		ready:  make(chan struct{}),
	}
	repo.RegisterProvider(prov)

	return prov, nil
}

func (ep *EnvProvider) Name() string      { return "env" }
func (ep *EnvProvider) Depends() []string { return []string{"default"} }
func (ep *EnvProvider) Weight() int       { return ep.weight }

func (ep *EnvProvider) SetUp(repo *Repository) error {
	defer close(ep.ready)
	registry := make(map[string]types.Value)
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
			repo.RegisterKey(types.NewKey(k), ep)
		}
	}

	ep.registry = registry

	return nil
}

func (ep *EnvProvider) TearDown(_ *Repository) error { return nil }

func (ep *EnvProvider) Get(key types.Key) (*types.KeyValue, bool) {
	<-ep.ready
	if val, ok := ep.registry[key.String()]; ok {
		return &types.KeyValue{key, val}, ok
	}
	return nil, false
}

func canonise(key string) string {
	return strings.ToLower(strings.Replace(key, "_", ".", -1))
}
