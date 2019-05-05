package cfg

import (
	"fmt"
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/awesome-flow/flow/pkg/types"
)

func TestCliProviderSetUp(t *testing.T) {
	tests := []struct {
		name     string
		registry map[string]types.Value
		wantRegs []string
	}{
		{
			"An empty map",
			map[string]types.Value{},
			[]string{},
		},
		{
			"A sample map",
			map[string]types.Value{
				"foo.bar": 42,
				"foo.baz": true,
				"moo":     []string{"a", "b", "c"},
			},
			[]string{"foo.bar", "foo.baz", "moo"},
		},
	}

	t.Parallel()

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {

			// Redefined function
			oldRegFlags := regFlags
			regFlags = func(cp *CliProvider) {
				for k, v := range testCase.registry {
					cp.registry[k] = v
				}
			}

			repo := NewRepository()
			prov, err := NewCliProvider(repo, 0)
			if err != nil {
				t.Fatalf("Failed to initialize a new cli provider: %s", err)
			}
			if err := prov.SetUp(repo); err != nil {
				t.Fatalf("Failed to set up cli provider: %s", err)
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

			regFlags = oldRegFlags
		})
	}
}

func TestCliProviderSet(t *testing.T) {
	tests := []struct {
		name         string
		setVal       string
		wantRegistry map[string]types.Value
		wantErr      error
	}{
		{
			"A bool flag",
			"foo",
			map[string]types.Value{"foo": true},
			nil,
		},
		{
			"A simple value",
			"foo=bar",
			map[string]types.Value{"foo": "bar"},
			nil,
		},
		{
			"Way too many = signs",
			"foo=bar=baz",
			map[string]types.Value{},
			fmt.Errorf("Possibly malformed flag (way too many `=`): %q", "foo=bar=baz"),
		},
	}

	t.Parallel()

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			repo := NewRepository()
			prov, err := NewCliProvider(repo, 0)
			if err != nil {
				t.Fatalf("Failed to instantiate a new cli provider: %s", err)
			}
			err = prov.Set(testCase.setVal)
			if !reflect.DeepEqual(err, testCase.wantErr) {
				t.Fatalf("Unexpected error while calling CliProvider.Set(%q): %s, want: %s", testCase.setVal, err, testCase.wantErr)
			}
			if !reflect.DeepEqual(prov.registry, testCase.wantRegistry) {
				t.Fatalf("Unexpected state for CliProvider.registry: want: %#v, got: %#v", testCase.wantRegistry, prov.registry)
			}
		})
	}
}
