package cfg

import (
	"fmt"
	"io/ioutil"

	"github.com/awesome-flow/flow/pkg/types"
	fsnotify "github.com/fsnotify/fsnotify"
	yaml "gopkg.in/yaml.v2"
)

// Redefined in tests
var readRaw = func(source string) (map[interface{}]interface{}, error) {
	out := make(map[interface{}]interface{})
	data, err := ioutil.ReadFile(source)
	if err != nil {
		return nil, fmt.Errorf("failed to read yaml config file %q: %s", source, err)
	}
	if err := yaml.Unmarshal(data, &out); err != nil {
		return nil, err
	}
	return out, nil
}

type YamlProvider struct {
	weight   int
	source   string
	options  *YamlProviderOptions
	watcher  *fsnotify.Watcher
	registry map[string]types.Value
	ready    chan struct{}
}

type YamlProviderOptions struct {
	Watch bool
}

var _ Provider = (*YamlProvider)(nil)

func NewYamlProvider(repo *Repository, weight int) (*YamlProvider, error) {
	return NewYamlProviderWithOptions(repo, weight, &YamlProviderOptions{})
}

func NewYamlProviderWithOptions(repo *Repository, weight int, options *YamlProviderOptions) (*YamlProvider, error) {
	return NewYamlProviderFromSource(repo, weight, options, "")
}

func NewYamlProviderFromSource(repo *Repository, weight int, options *YamlProviderOptions, source string) (*YamlProvider, error) {
	prov := &YamlProvider{
		source:   source,
		weight:   weight,
		options:  options,
		registry: make(map[string]types.Value),
		ready:    make(chan struct{}),
	}
	repo.RegisterProvider(prov)
	return prov, nil
}

func (yp *YamlProvider) Name() string      { return "yaml" }
func (yp *YamlProvider) Depends() []string { return []string{"cli", "env"} }
func (yp *YamlProvider) Weight() int       { return yp.weight }

func (yp *YamlProvider) SetUp(repo *Repository) error {
	defer close(yp.ready)

	if len(yp.source) == 0 {
		source, ok := repo.Get(types.NewKey(CfgPathKey))
		if !ok {
			return fmt.Errorf("Failed to get yaml config path from repo")
		}
		yp.source = source.(string)
	}

	// if _, err := os.Stat(yp.source); err != nil {
	// 	return fmt.Errorf("failed to read yaml config %q: %s", yp.source, err)
	// }

	if yp.options.Watch {
		watcher, err := fsnotify.NewWatcher()
		if err != nil {
			return fmt.Errorf("failed to start a yaml watcher: %s", err)
		}
		if err := watcher.Add(yp.source); err != nil {
			return fmt.Errorf("failed to add a new watchable file %q: %s", yp.source, err)
		}
		yp.watcher = watcher

		go yp.watch()
	}

	rawData, err := readRaw(yp.source)
	if err != nil {
		return err
	}
	for k, v := range flatten(rawData) {
		yp.registry[k] = v
		if repo != nil {
			if err := repo.RegisterKey(types.NewKey(k), yp); err != nil {
				return err
			}
		}
	}

	return nil
}

func flatten(in map[interface{}]interface{}) map[string]types.Value {
	out := make(map[string]types.Value)
	for k, v := range in {
		if vmap, ok := v.(map[interface{}]interface{}); ok {
			for sk, sv := range flatten(vmap) {
				out[k.(string)+types.KeySepCh+sk] = types.Value(sv)
			}
		} else {
			out[k.(string)] = types.Value(v)
		}
	}
	return out
}

func (yp *YamlProvider) watch() {
	for event := range yp.watcher.Events {
		fmt.Printf("Received a fsnotify event: %#v", event)
		//TODO (olegs): not implemented
		// if event.Op&fsnotify.Write != 1 {
		// 	continue
		// }
	}
}

func (yp *YamlProvider) TearDown(repo *Repository) error {
	if yp.watcher != nil {
		if err := yp.watcher.Close(); err != nil {
			return fmt.Errorf("failed to terminate the yaml watcher: %q", err)
		}
	}
	return nil
}

func (yp *YamlProvider) Get(key types.Key) (*types.KeyValue, bool) {
	<-yp.ready
	if v, ok := yp.registry[key.String()]; ok {
		return &types.KeyValue{Key: key, Value: v}, ok
	}
	return nil, false
}
