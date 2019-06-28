package cfg

import (
	"flag"
	"fmt"
	"strings"

	"github.com/awesome-flow/flow/pkg/types"
)

var (
	cfgFile    string
	pluginPath string
)

// Redefined in tests
var regFlags = func(cp *CliProvider) {
	if !flag.Parsed() {
		flag.StringVar(&cfgFile, CfgPathKey, "", "Config file path")
		flag.StringVar(&pluginPath, PluginPathKey, "", "Plugin folder path")
		flag.Var(cp, "o", "Extra options")
		flag.Parse()
		if len(cfgFile) > 0 {
			cp.registry[CfgPathKey] = cfgFile
		}
		if len(pluginPath) > 0 {
			cp.registry[PluginPathKey] = pluginPath
		}
	}
}

// CliProvider serves command-line flag values. By default, it registers a few
// basic flags, backing a full range of config keys by -o attribute.
type CliProvider struct {
	weight   int
	registry map[string]types.Value
	ready    chan struct{}
}

var _ Provider = (*CliProvider)(nil)
var _ flag.Value = (*CliProvider)(nil)

// NewCliProvider returns a new instance of CliProvider.
func NewCliProvider(repo *Repository, weight int) (*CliProvider, error) {
	prov := &CliProvider{
		weight:   weight,
		registry: make(map[string]types.Value),
		ready:    make(chan struct{}),
	}
	repo.RegisterProvider(prov)
	return prov, nil
}

// Name returns provider name: cli
func (cp *CliProvider) Name() string { return "cli" }

// Depends returns the list of provider dependencies: default
func (cp *CliProvider) Depends() []string { return []string{"default"} }

//Weight returns the provider weight
func (cp *CliProvider) Weight() int { return cp.weight }

// String satisfies flag.Value() interface
func (cp *CliProvider) String() string { return fmt.Sprintf("%v", cp.registry) }

// Set satisfies flag.Value() interface
func (cp *CliProvider) Set(val string) error {
	if chunks := strings.Split(val, "="); len(chunks) > 2 {
		return fmt.Errorf("Possibly malformed flag (way too many `=`): %q", val)
	} else if len(chunks) == 2 {
		cp.registry[chunks[0]] = chunks[1]
	} else {
		cp.registry[val] = true
	}
	return nil
}

// SetUp registers a bunch of command line flags (if not registered).
// Flag list:
// * -config.path: the config file location
// * -plugins.path: the plugin folder location
// * -o: extra options, ex: -o system.maxproc=4 -o pipeline.tcp_rcv.connect=udp
func (cp *CliProvider) SetUp(repo *Repository) error {
	defer close(cp.ready)
	regFlags(cp)
	for k := range cp.registry {
		if err := repo.RegisterKey(types.NewKey(k), cp); err != nil {
			return err
		}
	}
	return nil
}

// TearDown is a no-op operation for CliProvider
func (cp *CliProvider) TearDown(*Repository) error { return nil }

// Get is the primary method for fetching values from the cli registry
func (cp *CliProvider) Get(key types.Key) (*types.KeyValue, bool) {
	<-cp.ready
	if v, ok := cp.registry[key.String()]; ok {
		return &types.KeyValue{Key: key, Value: v}, ok
	}
	return nil, false
}
