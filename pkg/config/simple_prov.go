package config

import "fmt"

type SimpleProv struct {
	key   string
	value interface{}
}

var _ Provider = &SimpleProv{}

func NewSimpleProv(key string, value interface{}) *SimpleProv {
	return &SimpleProv{key: key, value: value}
}

func (sp *SimpleProv) DependsOn() []string {
	return nil
}

func (sp *SimpleProv) GetName() string {
	return fmt.Sprintf("simple_provider[%v]", sp.value)
}

func (sp *SimpleProv) GetOptions() ProviderOptions {
	return ProviderOptionsNone
}

func (sp *SimpleProv) GetValue(key string) (interface{}, bool) {
	if key == sp.key {
		return sp.value, true
	}
	return nil, false
}

func (sp *SimpleProv) GetWeight() uint32 {
	return 100
}

func (sp *SimpleProv) Resolve() error {
	return nil
}

func (sp *SimpleProv) Setup() error {
	Register(sp.key, sp)
	return nil
}
