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
		// tp, stands for type can be either value or flag.
		// The cast is expected to be performed by the client.
		{"value", "config.file", "config", "",
			"Location of the config file"},
		{"value", "plugin.path", "plugin-path", "",
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
		case "value":
			settings[flg.name] = flag.String(flg.param, flg.dflt.(string), flg.descr)
		case "flag":
			settings[flg.name] = flag.Bool(flg.param, flg.dflt.(bool), flg.descr)
			// add more types if needed
		}
		if err := Register(flg.name, c); err != nil {
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
	if v, ok := s[key]; ok {
		if vConv, convOk := v.(*string); convOk {
			if *vConv == "" {
				return nil, false
			}
			return *vConv, true
		} else if vConv, convOk := v.(*int); convOk {
			return *vConv, true
		} else if vConv, convOk := v.(*bool); convOk {
			return *vConv, true
		}
	}

	return nil, false
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
