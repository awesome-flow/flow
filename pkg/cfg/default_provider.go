package cfg

import (
	"github.com/awesome-flow/flow/pkg/types"
)

// DefaultProvider represents a set of default values.
// Prefer keeping defaults over providing default values local to other
// providers as it guarantees presence of the default values indiffirent to
// the provider set that have been activated.
type DefaultProvider struct {
	weight   int
	registry map[string]types.Value
	ready    chan struct{}
}

var (
	defaults map[string]types.Value
)

func init() {
	defaults = map[string]types.Value{
		CfgPathKey:        "/etc/flowd/flow-config.yaml",
		PluginPathKey:     "/etc/flowd/plugins",
		"system.maxprocs": 1,
	}
}

var _ Provider = (*DefaultProvider)(nil)

// NewDefaultProvider is a constructor for DefaultProvider.
func NewDefaultProvider(repo *Repository, weight int) (*DefaultProvider, error) {
	return NewDefaultProviderWithRegistry(repo, weight, defaults)
}

// NewDefaultProviderWithRegistry is an alternative constructor for
// DefaultProvider. Accepts an extra registry argument as a complete replacement
// for the default one.
func NewDefaultProviderWithRegistry(repo *Repository, weight int, registry map[string]types.Value) (*DefaultProvider, error) {
	prov := &DefaultProvider{
		weight:   weight,
		registry: registry,
		ready:    make(chan struct{}),
	}
	repo.RegisterProvider(prov)
	return prov, nil
}

// Name returns provider name: default
func (dp *DefaultProvider) Name() string { return "default" }

// Depends returns the list of provider dependencies: none
func (dp *DefaultProvider) Depends() []string { return []string{} }

// Weight returns the provider weight
func (dp *DefaultProvider) Weight() int { return dp.weight }

// SetUp registers all keys from the registry in the repo
func (dp *DefaultProvider) SetUp(repo *Repository) error {
	defer close(dp.ready)
	for k := range dp.registry {
		repo.RegisterKey(types.NewKey(k), dp)
	}
	return nil
}

// TearDown is a no-op operation for DefaultProvider
func (dp *DefaultProvider) TearDown(*Repository) error { return nil }

// Get is the primary method for fetching values from the default registry
func (dp *DefaultProvider) Get(key types.Key) (*types.KeyValue, bool) {
	<-dp.ready
	if val, ok := dp.registry[key.String()]; ok {
		return &types.KeyValue{Key: key, Value: val}, ok
	}
	return nil, false
}
