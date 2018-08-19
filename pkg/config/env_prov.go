package config

import (
	"os"
	"strings"
	"sync/atomic"
)

type envProv struct {
	settings *atomic.Value
}

var (
	envInst *envProv
)

func init() {
	envInst = &envProv{
		settings: &atomic.Value{},
	}
	envInst.Setup()
}

func (e *envProv) Setup() error {
	settings := make(map[string]string)
	for _, kvPair := range os.Environ() {
		kvSplit := strings.SplitN(kvPair, "=", 2)
		k, v := kvSplit[0], kvSplit[1]
		if strings.HasPrefix(k, "_") {
			continue
		}
		kDots := strings.Join(strings.Split(k, "_"), ".")
		kDots = strings.ToLower(kDots)
		settings[kDots] = v
		Register(kDots, e)
	}
	e.settings.Store(settings)
	return nil
}

func (e *envProv) GetOptions() ProviderOptions {
	return 0
}

func (e *envProv) GetValue(key string) (interface{}, bool) {
	v, ok := e.settings.Load().(map[string]string)[key]
	return v, ok
}

func (e *envProv) GetWeight() uint32 {
	return 40
}

func (e *envProv) Resolve() error {
	return nil
}

func (e *envProv) DependsOn() []string {
	return []string{}
}

func (e *envProv) GetName() string {
	return "env"
}
