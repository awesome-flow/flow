package cfg

import (
	"github.com/awesome-flow/flow/pkg/cast"
)

type DefaultProvider struct {
	weight   int
	registry map[string]cast.Value
	ready    chan struct{}
}

var (
	defaults map[string]cast.Value
)

func init() {
	defaults = map[string]cast.Value{
		CfgPathKey:    "/etc/flowd/flow-config.yaml",
		PluginPathKey: "/etc/flowd/plugins",
	}
}

var _ Provider = (*DefaultProvider)(nil)

func NewDefaultProvider(repo *Repository, weight int) (*DefaultProvider, error) {
	return NewDefaultProviderWithRegistry(repo, weight, defaults)
}

func NewDefaultProviderWithRegistry(repo *Repository, weight int, registry map[string]cast.Value) (*DefaultProvider, error) {
	prov := &DefaultProvider{
		weight:   weight,
		registry: registry,
		ready:    make(chan struct{}),
	}
	repo.RegisterProvider(prov)
	return prov, nil
}

func (dp *DefaultProvider) Name() string      { return "default" }
func (dp *DefaultProvider) Depends() []string { return []string{} }
func (dp *DefaultProvider) Weight() int       { return dp.weight }

func (dp *DefaultProvider) SetUp(repo *Repository) error {
	defer close(dp.ready)
	for k := range dp.registry {
		repo.Register(cast.NewKey(k), dp)
	}
	return nil
}

func (dp *DefaultProvider) TearDown(_ *Repository) error { return nil }

func (dp *DefaultProvider) Get(key cast.Key) (*cast.KeyValue, bool) {
	<-dp.ready
	if val, ok := dp.registry[key.String()]; ok {
		return &cast.KeyValue{key, val}, ok
	}
	return nil, false
}
