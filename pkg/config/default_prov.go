package config

type defaultProv struct{}

var defaultInst *defaultProv

func init() {
	defaultInst = &defaultProv{}
	defaultInst.Setup()
}

var (
	defaultConfigs = map[string]string{
		"config.file":      "/etc/flowd/flow-config.yml",
		"flow.plugin.path": "/etc/flowd/plugins",
	}
)

func (d *defaultProv) Setup() error {
	for k := range defaultConfigs {
		if err := Register(k, d); err != nil {
			return err
		}
	}
	return nil
}

func (d *defaultProv) GetOptions() ProviderOptions {
	return 0
}

func (d *defaultProv) GetValue(key string) (interface{}, bool) {
	res, ok := defaultConfigs[key]
	return res, ok
}

func (d *defaultProv) GetWeight() uint32 {
	return 0
}

func (d *defaultProv) Resolve() error {
	return nil
}

func (d *defaultProv) DependsOn() []string {
	return []string{}
}

func (c *defaultProv) GetName() string {
	return "default"
}
