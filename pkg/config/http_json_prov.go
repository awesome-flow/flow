package config

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"sync"
	"sync/atomic"
)

type HttpJsonOpt uint32

const (
	HttpJsonOptPeriodic HttpJsonOpt = 1 << iota
)

type HttpJsonProv struct {
	fetchUrl  string
	opts      HttpJsonOpt
	once      *sync.Once
	value     *atomic.Value
	lastErr   *atomic.Value
	container interface{}
}

func RegisterHttpJsonProv(cfgPath string, fetchUrl string,
	container interface{}, opts HttpJsonOpt) (*HttpJsonProv, error) {
	hjp := &HttpJsonProv{
		fetchUrl:  fetchUrl,
		opts:      opts,
		once:      &sync.Once{},
		value:     &atomic.Value{},
		lastErr:   &atomic.Value{},
		container: container,
	}
	return hjp, Register(cfgPath, hjp)
}

func (hjp *HttpJsonProv) Setup() error {
	return nil
}

func (hjp *HttpJsonProv) GetOptions() ProviderOptions {
	return ProviderOptionsFileCache | ProviderOptionsTrustOldCache
}

func (hjp *HttpJsonProv) GetValue(key string) (interface{}, bool) {
	if err := hjp.Resolve(); err != nil {
		if val := hjp.value.Load(); val != nil {
			return val, true
		}
		return nil, false
	}
	return hjp.value.Load(), true
}

func (hjp *HttpJsonProv) GetWeight() uint32 {
	return 10
}

func (hjp *HttpJsonProv) Resolve() error {
	hjp.once.Do(func() {
		rawRes, fetchErr := http.Get(hjp.fetchUrl)
		if fetchErr != nil {
			hjp.lastErr.Store(fetchErr)
			return
		}
		if rawRes.StatusCode != http.StatusOK {
			hjp.lastErr.Store(
				fmt.Errorf("Unexpected response code returned: %d", rawRes.StatusCode))
		}
		body, readErr := ioutil.ReadAll(rawRes.Body)
		if readErr != nil {
			hjp.lastErr.Store(readErr)
			return
		}
		if len(body) == 0 {
			hjp.lastErr.Store(fmt.Errorf("Empty response from the server"))
			return
		}
		parseErr := json.Unmarshal(body, hjp.container)
		if parseErr != nil {
			hjp.lastErr.Store(parseErr)
			return
		}
		hjp.value.Store(hjp.container)
		hjp.lastErr = &atomic.Value{}
	})
	if err := hjp.lastErr.Load(); err != nil {
		return err.(error)
	}
	return nil
}

func (hjp *HttpJsonProv) DependsOn() []string {
	return []string{}
}

func (hjp *HttpJsonProv) GetName() string {
	return "http_json:" + hjp.fetchUrl
}
