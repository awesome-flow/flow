package config

import (
	"io/ioutil"

	yaml "gopkg.in/yaml.v2"
)

type yamlProv struct {
	cfg *YAMLConfig
}

type CfgBlockComponent struct {
	Module      string
	Plugin      string
	Constructor string
	Params      map[string]interface{}
}

type CfgBlockPipeline struct {
	Links   []string
	Connect string
	Routes  map[string]string
}

type CfgBlockSystemAdmin struct {
	Enabled  bool
	BindAddr string `yaml:"bind_addr"`
}
type CfgBlockSystem struct {
	Maxprocs int
	Admin    CfgBlockSystemAdmin
}

type YAMLConfig struct {
	System     *CfgBlockSystem
	Components map[string]CfgBlockComponent
	Pipeline   map[string]CfgBlockPipeline
}

var (
	yamlInst *yamlProv
)

func init() {
	yamlInst = &yamlProv{}
	yamlInst.Setup()
}

const (
	YML_CFG_KEY_SYS  = "global.system"
	YML_CFG_KEY_COMP = "global.components"
	YML_CFG_KEY_PPL  = "global.pipeline"
)

func (y *yamlProv) Setup() error {
	keys := []string{
		YML_CFG_KEY_SYS,
		YML_CFG_KEY_COMP,
		YML_CFG_KEY_PPL,
	}
	for _, k := range keys {
		if err := Register(k, y); err != nil {
			return err
		}
	}
	return nil
}

func (y *yamlProv) GetOptions() ProviderOptions {
	return 0
}

func (y *yamlProv) GetValue(key string) (interface{}, bool) {
	switch key {
	case YML_CFG_KEY_SYS:
		if y.cfg.System == nil {
			return nil, false
		}
		return y.cfg.System, true
	case YML_CFG_KEY_COMP:
		if y.cfg.Components == nil {
			return nil, false
		}
		return y.cfg.Components, true
	case YML_CFG_KEY_PPL:
		if y.cfg.Pipeline == nil {
			return nil, false
		}
		return y.cfg.Pipeline, true
	default:
		return nil, false
	}
}

func (y *yamlProv) GetWeight() uint32 {
	return 20
}

func (y *yamlProv) Resolve() error {
	pathIntf, ok := Get("config.file")
	if !ok {
		panic("Could not resolve the config file: config.file setting is missing. " +
			"Provide as a cli argument -config_file=... or env variable CONFIG_FILE=...")
	}

	path := pathIntf.(string)

	data, readErr := ioutil.ReadFile(path)
	if readErr != nil {
		return readErr
	}

	y.cfg = &YAMLConfig{}
	yamlErr := yaml.Unmarshal(data, y.cfg)
	if yamlErr != nil {
		return yamlErr
	}

	return nil
}

func (y *yamlProv) DependsOn() []string {
	return []string{"cli", "env"}
}

func (y *yamlProv) GetName() string {
	return "yaml"
}
