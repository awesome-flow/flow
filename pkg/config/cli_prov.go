package config

import (
	"flag"
	"sync/atomic"
)

type cliProv struct {
	settings *atomic.Value
}

var (
	cliInst *cliProv
)

var (
	knownFlags = []struct {
		tp    string
		name  string
		param string
		dflt  interface{}
		descr string
	}{
		{"string", "config.file", "config", "/etc/flowd/flow-config.yml",
			"Location of the config file"},
		{"string", "flow.plugin.path", "flow-plugin-path", "/etc/flowd/plugins",
			"Flow plugin path"},
	}
)

func init() {
	cliInst = &cliProv{
		settings: &atomic.Value{},
	}
	cliInst.Setup()
}

func (c *cliProv) Setup() error {
	settings := make(map[string]interface{})
	for _, flg := range knownFlags {
		switch flg.tp {
		case "string":
			settings[flg.name] = flag.String(flg.param, flg.dflt.(string), flg.descr)
		case "int":
			settings[flg.name] = flag.Int(flg.param, flg.dflt.(int), flg.descr)
		case "bool":
			settings[flg.name] = flag.Bool(flg.param, flg.dflt.(bool), flg.descr)
			// add more types if needed
		}
		if err := Register(flg.name, cliInst); err != nil {
			return err
		}
	}
	c.settings.Store(settings)

	return nil
}

func (c *cliProv) GetOptions() ProviderOptions {
	return 0
}

func (c *cliProv) GetValue(key string) (interface{}, bool) {
	s := c.settings.Load().(map[string]interface{})
	v, ok := s[key]
	return v, ok
}

func (c *cliProv) GetWeight() uint32 {
	return 50
}

func (c *cliProv) Resolve() error {
	flag.Parse()
	return nil
}

func (c *cliProv) DependsOn() []string {
	return []string{}
}

func (c *cliProv) GetName() string {
	return "cli"
}
