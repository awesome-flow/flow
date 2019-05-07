package cfg

import (
	"reflect"
	"sort"
	"strings"
	"testing"

	"gopkg.in/yaml.v2"
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

func TestYamlProviderSetUp(t *testing.T) {
	tests := []struct {
		name     string
		src      []byte
		options  *YamlProviderOptions
		wantRegs []string
	}{
		{
			"empty yaml",
			[]byte(""),
			&YamlProviderOptions{},
			[]string{},
		},
		{
			"Sample yaml",
			[]byte(sampleYaml),
			&YamlProviderOptions{},
			[]string{
				"system.maxprocs",
				"system.admin.enabled",
				"components.udp_rcv.params.bind_addr",
				"components.udp_rcv.module",
				"components.tcp_sink_7223.params.bind_addr",
				"components.tcp_sink_7223.module",
				"components.tcp_sink_7222.params.bind_addr",
				"components.tcp_sink_7222.module",
				"components.fanout.module",
				"pipeline.udp_rcv.connect",
				"pipeline.fanout.links",
			},
		},
	}

	t.Parallel()

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {

			// Redefining the original value
			oldReadRaw := readRaw
			readRaw = func(source string) (map[interface{}]interface{}, error) {
				out := make(map[interface{}]interface{})
				if err := yaml.Unmarshal(testCase.src, &out); err != nil {
					return nil, err
				}
				return out, nil
			}

			repo := NewRepository()
			prov, err := NewYamlProviderFromSource(repo, 0, testCase.options, "dummy.dummy")
			if err != nil {
				t.Fatalf("Failed to initialize a new yaml provider: %s", err)
			}
			if err := prov.SetUp(repo); err != nil {
				t.Fatalf("Failed to set up yaml provider: %s", err)
			}
			gotRegs := flattenRepo(repo)
			for _, k := range testCase.wantRegs {
				provs, ok := gotRegs[k]
				if !ok {
					t.Fatalf("Failed to find a registration for key %q", k)
				}
				if !reflect.DeepEqual(provs, []Provider{prov}) {
					t.Fatalf("Unexpected provider list for key %q: %#v, want: %#v", k, provs, []Provider{prov})
				}
				delete(gotRegs, k)
			}
			if len(gotRegs) > 0 {
				extraKeys := make([]string, 0, len(gotRegs))
				for k := range gotRegs {
					extraKeys = append(extraKeys, k)
				}
				sort.Strings(extraKeys)
				t.Fatalf("Unexpected registration keys: %s", strings.Join(extraKeys, ", "))
			}

			readRaw = oldReadRaw
		})
	}
}
