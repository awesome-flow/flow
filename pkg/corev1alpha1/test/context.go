package test

import (
	"github.com/awesome-flow/flow/pkg/cfg"
	core "github.com/awesome-flow/flow/pkg/corev1alpha1"
	"github.com/awesome-flow/flow/pkg/types"
)

func NewContextWithConfig(config map[string]interface{}) (*core.Context, error) {
	repo := cfg.NewRepository()
	for k, v := range config {
		if _, err := cfg.NewScalarConfigProvider(
			&types.KeyValue{
				Key:   types.NewKey(k),
				Value: v,
			},
			repo,
			42, // doesn't matter
		); err != nil {
			return nil, err
		}
	}

	ctx, err := core.NewContext(core.NewConfig(repo))
	if err != nil {
		return nil, err
	}

	return ctx, nil
}
