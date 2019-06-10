package corev1alpha1

import "github.com/awesome-flow/flow/pkg/cfg"

type Config struct {
	repo *cfg.Repository
}

func NewConfig(repo *cfg.Repository) *Config {
	return &Config{
		repo: repo,
	}
}

func (config *Config) Repo() *cfg.Repository {
	return config.repo
}
