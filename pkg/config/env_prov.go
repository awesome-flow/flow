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

func dotize(key string) string {
	return strings.Join(strings.Split(key, "_"), ".")
}

func (e *envProv) Setup() error {
	settings := make(map[string]interface{})
	for _, kvPair := range os.Environ() {
		if strings.HasPrefix(kvPair, "_") {
			continue
		}
		var k string
		var v interface{}
		if ix := strings.Index(kvPair, "="); ix != -1 {
			k, v = kvPair[:ix], kvPair[ix+1:]
		} else {
			k, v = kvPair, true
		}
		dotkey := strings.ToLower(dotize(k))
		settings[dotkey] = v
		Register(dotkey, e)
	}
	e.settings.Store(settings)
	return nil
}

func (e *envProv) GetOptions() ProviderOptions {
	return 0
}

func (e *envProv) GetValue(key string) (interface{}, bool) {
	v, ok := e.settings.Load().(map[string]interface{})[key]
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
