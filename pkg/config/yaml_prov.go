package config

import (
	"io/ioutil"

	yaml "gopkg.in/yaml.v2"
)

type yamlProv struct {
	cfg *YAMLConfig
}

var _ Provider = &yamlProv{}

type CfgBlockComponent struct {
	Constructor string
	Module      string
	Params      map[string]interface{}
	Plugin      string
}

type CfgBlockPipeline struct {
	Connect string
	Links   []string
	Routes  map[string]string
}

func (cbp CfgBlockPipeline) IsDisconnected() bool {
	return len(cbp.Links) == 0 &&
		len(cbp.Connect) == 0 &&
		len(cbp.Routes) == 0
}

type CfgBlockSystemAdmin struct {
	BindAddr string `yaml:"bind_addr"`
	Enabled  bool
}
type CfgBlockSystemMetricsReceiver struct {
	Params map[string]string
	Type   string
}
type CfgBlockSystemMetrics struct {
	Enabled  bool
	Interval int
	Receiver CfgBlockSystemMetricsReceiver
}
type CfgBlockSystem struct {
	Admin    CfgBlockSystemAdmin
	Maxprocs int
	Metrics  CfgBlockSystemMetrics
}

type YAMLConfig struct {
	Components map[string]CfgBlockComponent
	Pipeline   map[string]CfgBlockPipeline
	System     *CfgBlockSystem
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
		YML_CFG_KEY_SYS + ".maxprocs",
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
	return ProviderOptionsNone
}

func (y *yamlProv) GetValue(key string) (interface{}, bool) {
	if y.cfg == nil {
		return nil, false
	}
	switch key {
	case YML_CFG_KEY_SYS:
		if y.cfg.System == nil {
			return nil, false
		}
		return y.cfg.System, true

	case YML_CFG_KEY_SYS + ".maxprocs":
		if y.cfg.System == nil {
			return nil, false
		}
		return y.cfg.System.Maxprocs, true
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
			"Provide as a cli argument -config-file=... or env variable CONFIG_FILE=...")
	}

	var path string
	if v, ok := pathIntf.(string); ok {
		path = v
	} else if v, ok := pathIntf.(*string); ok {
		path = *v
	}

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
	return []string{"cli", "env", "default"}
}

func (y *yamlProv) GetName() string {
	return "yaml"
}
