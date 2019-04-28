package cfg

import (
	"flag"
	"fmt"
	"strings"

	"github.com/awesome-flow/flow/pkg/types"
)

type CliProvider struct {
	weight   int
	registry map[string]types.Value
	ready    chan struct{}
}

var _ Provider = (*CliProvider)(nil)

func NewCliProvider(repo *Repository, weight int) (*CliProvider, error) {
	prov := &CliProvider{
		weight:   weight,
		registry: make(map[string]types.Value),
		ready:    make(chan struct{}),
	}
	repo.RegisterProvider(prov)
	return prov, nil
}

func (cp *CliProvider) Name() string      { return "cli" }
func (cp *CliProvider) Depends() []string { return []string{"default"} }
func (cp *CliProvider) Weight() int       { return cp.weight }

func (cp *CliProvider) String() string { return fmt.Sprintf("%v", cp.registry) }
func (cp *CliProvider) Set(val string) error {
	if chunks := strings.Split(val, "="); len(chunks) > 1 {
		cp.registry[chunks[0]] = chunks[1]
	} else {
		cp.registry[val] = true
	}
	return nil
}

var (
	cfgFile    string
	pluginPath string
)

func (cp *CliProvider) SetUp(repo *Repository) error {
	defer close(cp.ready)
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
	for k := range cp.registry {
		repo.RegisterKey(types.NewKey(k), cp)
	}
	return nil
}

func (cp *CliProvider) TearDown(repo *Repository) error {
	return nil
}

func (cp *CliProvider) Get(key types.Key) (*types.KeyValue, bool) {
	<-cp.ready
	if v, ok := cp.registry[key.String()]; ok {
		return &types.KeyValue{key, v}, ok
	}
	return nil, false
}
