package corev1alpha1

import "github.com/awesome-flow/flow/pkg/cfg"

type Config struct {
	*cfg.Repository
}

var _ Runner = (*Config)(nil)

func NewConfig(repo *cfg.Repository) *Config {
	return &Config{repo}
}

func (config *Config) Start() error {
	return config.SetUp()
}

func (config *Config) Stop() error {
	return config.TearDown()
}
