package cfg

import (
	"io/ioutil"
	"os"
	"reflect"
	"testing"

	"github.com/awesome-flow/flow/pkg/types"
)

const (
	DefaultWeight = 10
	sampleYaml    = `
system:
  maxprocs: 4
  admin:
    enabled: true
components:
  udp_rcv:
    module: receiver.udp
    params:
      bind_addr: localhost:3101
  fanout:
    module: link.fanout
  tcp_sink_7222:
    module: sink.tcp
    params:
      bind_addr: localhost:7222
  tcp_sink_7223:
    module: sink.tcp
    params:
      bind_addr: localhost:7223
pipeline:
  udp_rcv:
    connect: fanout
  fanout:
    links:
      - tcp_sink_7222
      - tcp_sink_7223
`
)

func tmpyamlcfg() (*os.File, error) {
	f, err := ioutil.TempFile(".", "yaml-provider")
	if err != nil {
		return nil, err
	}
	if _, err := f.Write([]byte(sampleYaml)); err != nil {
		return nil, err
	}

	return f, nil
}

func Test_readRaw(t *testing.T) {
	f, err := tmpyamlcfg()
	if err != nil {
		t.Fatalf("Failed to create tmp file: %s", err)
	}

	defer os.Remove(f.Name())
	repo := NewRepository()

	yp, err := NewYamlProviderFromSource(repo, DefaultWeight, &YamlProviderOptions{}, f.Name())
	if err != nil {
		t.Fatalf("Failed to instantiate a yaml provider: %s", err)
	}
	if err := yp.SetUp(nil); err != nil {
		t.Fatalf("Failed to call provider.SetUp: %s", err)
	}

	got, err := yp.readRaw()
	if err != nil {
		t.Fatalf("Failed to read config data: %s", err)
	}

	expected := map[interface{}]interface{}{
		"system": map[interface{}]interface{}{
			"maxprocs": 4,
			"admin": map[interface{}]interface{}{
				"enabled": true,
			},
		},
		"components": map[interface{}]interface{}{
			"udp_rcv": map[interface{}]interface{}{
				"module": "receiver.udp",
				"params": map[interface{}]interface{}{
					"bind_addr": "localhost:3101",
				},
			},
			"fanout": map[interface{}]interface{}{
				"module": "link.fanout",
			},
			"tcp_sink_7222": map[interface{}]interface{}{
				"module": "sink.tcp",
				"params": map[interface{}]interface{}{
					"bind_addr": "localhost:7222",
				},
			},
			"tcp_sink_7223": map[interface{}]interface{}{
				"module": "sink.tcp",
				"params": map[interface{}]interface{}{
					"bind_addr": "localhost:7223",
				},
			},
		},
		"pipeline": map[interface{}]interface{}{
			"udp_rcv": map[interface{}]interface{}{
				"connect": "fanout",
			},
			"fanout": map[interface{}]interface{}{
				"links": []interface{}{
					"tcp_sink_7222",
					"tcp_sink_7223",
				},
			},
		},
	}
	if !reflect.DeepEqual(expected, got) {
		t.Fatalf("Unexpected value:\n\nwant: %#v\n\ngot: %#v", expected, got)
	}
}

func Test_SetUp(t *testing.T) {
	repo := NewRepository()

	f, err := tmpyamlcfg()
	if err != nil {
		t.Fatalf("Failed to create tmp file: %s", err)
	}
	defer os.Remove(f.Name())

	yp, err := NewYamlProviderFromSource(repo, DefaultWeight, &YamlProviderOptions{}, f.Name())
	if err != nil {
		t.Fatalf("Failed to instantiate new yaml provider: %s", err)
	}

	if err := yp.SetUp(repo); err != nil {
		t.Fatalf("failed to SetUp yaml provider: %s", err)
	}

	testsProv := map[string]struct {
		Ok bool
		V  interface{}
	}{
		"system.maxprocs":      {true, 4},
		"system.admin.enabled": {true, true},

		"components.udp_rcv.module":                 {true, "receiver.udp"},
		"components.udp_rcv.params.bind_addr":       {true, "localhost:3101"},
		"components.fanout.module":                  {true, "link.fanout"},
		"components.tcp_sink_7222.module":           {true, "sink.tcp"},
		"components.tcp_sink_7222.params.bind_addr": {true, "localhost:7222"},
		"components.tcp_sink_7223.module":           {true, "sink.tcp"},
		"components.tcp_sink_7223.params.bind_addr": {true, "localhost:7223"},

		"pipeline.udp_rcv.connect": {true, "fanout"},
		"pipeline.fanout.links":    {true, []interface{}{"tcp_sink_7222", "tcp_sink_7223"}},

		"foo":          {false, nil},
		"pipeline":     {false, nil},
		"system.admin": {false, nil},
	}

	for k, expected := range testsProv {
		t.Run(k, func(t *testing.T) {
			provKV, provOk := yp.Get(types.NewKey(k))
			if provOk != expected.Ok {
				t.Fatalf("expected %q to be present: %t, got: %t", k, expected.Ok, provOk)
			}
			if provOk {
				if !reflect.DeepEqual(expected.V, provKV.Value) {
					t.Fatalf("unexpected value for %q: got: %#v, want: %#v", k, provKV.Value, expected.V)
				}
			}
		})
	}

	testsRepo := map[string]struct {
		Ok bool
		V  interface{}
	}{
		"system.maxprocs":      {true, 4},
		"system.admin.enabled": {true, true},

		"components.udp_rcv.module":                 {true, "receiver.udp"},
		"components.udp_rcv.params.bind_addr":       {true, "localhost:3101"},
		"components.fanout.module":                  {true, "link.fanout"},
		"components.tcp_sink_7222.module":           {true, "sink.tcp"},
		"components.tcp_sink_7222.params.bind_addr": {true, "localhost:7222"},
		"components.tcp_sink_7223.module":           {true, "sink.tcp"},
		"components.tcp_sink_7223.params.bind_addr": {true, "localhost:7223"},

		"pipeline.udp_rcv.connect": {true, "fanout"},
		"pipeline.fanout.links":    {true, []interface{}{"tcp_sink_7222", "tcp_sink_7223"}},

		"foo": {false, nil},
		"pipeline": {true, map[string]types.Value{
			"fanout": map[string]types.Value{
				"links": []interface{}{
					"tcp_sink_7222",
					"tcp_sink_7223",
				},
			},
			"udp_rcv": map[string]types.Value{
				"connect": "fanout",
			},
		}},
		"components.udp_rcv": {true, map[string]types.Value{
			"module": "receiver.udp",
			"params": map[string]types.Value{
				"bind_addr": "localhost:3101",
			},
		}},
		"system.admin": {true, map[string]types.Value{
			"enabled": true,
		}},
	}

	for k, expected := range testsRepo {
		t.Run(k, func(t *testing.T) {
			repoV, repoOk := repo.Get(types.NewKey(k))
			if repoOk != expected.Ok {
				t.Fatalf("expected %q to be present in repo, got: %t, expected: %t", k, repoOk, expected.Ok)
			}
			if repoOk {
				if !reflect.DeepEqual(expected.V, repoV) {
					t.Fatalf("unexpected repo value for %q: got: %#v, want: %#v", k, repoV, expected.V)
				}
			}
		})
	}
}
