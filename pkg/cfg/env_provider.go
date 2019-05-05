package cfg

import (
	"os"
	"strings"

	"github.com/awesome-flow/flow/pkg/types"
)

// Redefined in tests
var envVars = func() []string {
	return os.Environ()
}

func canonise(key string) string {
	k := strings.Replace(key, "_", ".", -1)
	k = strings.Replace(k, "..", "_", -1)
	return strings.ToLower(k)
}

// EnvProvider reads special FLOW_ preffixed environment variables.
// The contract is:
// * Underscores are being transformed to dots in key part (before the first =).
// * There must be exactly 1 `=` sign.
// * Double underscores are converted to singulars and preserved with no dot-conversion.
type EnvProvider struct {
	weight   int
	registry map[string]types.Value
	ready    chan struct{}
}

var _ Provider = (*EnvProvider)(nil)

// NewEnvProvider returns a new instance of EnvProvider.
func NewEnvProvider(repo *Repository, weight int) (*EnvProvider, error) {
	prov := &EnvProvider{
		weight: weight,
		ready:  make(chan struct{}),
	}
	repo.RegisterProvider(prov)

	return prov, nil
}

// Name returns provider name: env
func (ep *EnvProvider) Name() string { return "env" }

// Depends returns provider dependencies: default
func (ep *EnvProvider) Depends() []string { return []string{"default"} }

// Weight returns provider weight
func (ep *EnvProvider) Weight() int { return ep.weight }

// SetUp takes the list of env vars and canonizes them before registration in
// repo. Env vars are expected to be in form FLOW_<K>=<v>. FLOW_ preffix
// would be cleared out.
func (ep *EnvProvider) SetUp(repo *Repository) error {
	defer close(ep.ready)
	registry := make(map[string]types.Value)
	var k string
	var v interface{}

	for _, kv := range envVars() {
		if !strings.HasPrefix(kv, "FLOW_") {
			continue
		}
		// Clear out "FLOW_"
		kv = kv[5:]
		if ix := strings.Index(kv, "="); ix != -1 {
			k, v = kv[:ix], kv[ix+1:]
		} else {
			k, v = kv, true
		}
		k = canonise(k)
		registry[k] = v
		if repo != nil {
			repo.RegisterKey(types.NewKey(k), ep)
		}
	}

	ep.registry = registry

	return nil
}

// TearDown is a no-op operation for CliProvider
func (ep *EnvProvider) TearDown(_ *Repository) error { return nil }

// Get is the primary method to fetch values from the provider registry.
func (ep *EnvProvider) Get(key types.Key) (*types.KeyValue, bool) {
	<-ep.ready
	if val, ok := ep.registry[key.String()]; ok {
		return &types.KeyValue{Key: key, Value: val}, ok
	}
	return nil, false
}
