package corev1alpha1

import (
	flowplugin "github.com/awesome-flow/flow/pkg/util/plugin"
)

type TestPlugin struct {
	Path string
	Name string
}

func (p *TestPlugin) Load() error {
	return nil
}

func (p *TestPlugin) Lookup(symName string) (flowplugin.Symbol, error) {
	return NewTestActor, nil
}
