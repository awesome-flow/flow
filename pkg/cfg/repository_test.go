package cfg

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/awesome-flow/flow/pkg/cast"
	"github.com/awesome-flow/flow/pkg/types"
)

func strptr(v string) *string { return &v }
func boolptr(v bool) *bool    { return &v }
func intptr(v int) *int       { return &v }

func flushMappers() {
	mappersMx.Lock()
	defer mappersMx.Unlock()
	mappers = cast.NewMapperNode()
}

type TestProv struct {
	val     types.Value
	weight  int
	isSetUp bool
}

func NewTestProv(val types.Value, weight int) *TestProv {
	return &TestProv{
		val:     val,
		weight:  weight,
		isSetUp: false,
	}
}

func (tp *TestProv) SetUp(_ *Repository) error {
	tp.isSetUp = true
	return nil
}

func (tp *TestProv) TearDown(_ *Repository) error { return nil }

func (tp *TestProv) Get(key types.Key) (*types.KeyValue, bool) {
	return &types.KeyValue{
		Key:   key,
		Value: tp.val,
	}, true
}

func (tp *TestProv) Weight() int       { return tp.weight }
func (tp *TestProv) Name() string      { return "test" }
func (tp *TestProv) Depends() []string { return []string{} }

func TestGetSingleProvider(t *testing.T) {
	repo := NewRepository()
	prov := NewTestProv(42, 10)
	key := types.NewKey("foo.bar.baz")
	repo.Register(key, prov)

	tests := []struct {
		key types.Key
		ok  bool
		val types.Value
	}{
		{
			key: types.NewKey("foo"),
			ok:  true,
			val: map[string]types.Value{
				"bar": map[string]types.Value{
					"baz": 42,
				},
			},
		},
		{
			key: types.NewKey("foo.bar"),
			ok:  true,
			val: map[string]types.Value{"baz": 42},
		},
		{
			key: types.NewKey("foo.bar.baz"),
			ok:  true,
			val: 42,
		},
		{
			key: types.NewKey("foo.bar.baz.boo"),
			ok:  false,
			val: nil,
		},
	}

	for _, testCase := range tests {
		val, ok := repo.Get(testCase.key)
		if ok != testCase.ok {
			t.Fatalf("Unexpected key %q lookup result: want %t, got: %t", testCase.key, testCase.ok, ok)
		}
		if !ok {
			continue
		}
		if !reflect.DeepEqual(val, testCase.val) {
			t.Fatalf("Unexpected value for key %q: want %v, got %v", testCase.key, testCase.val, val)
		}
	}
}

func TestTrioProviderSingleKey(t *testing.T) {
	repo := NewRepository()
	prov1 := NewTestProv(10, 10)
	prov2 := NewTestProv(20, 20)
	prov3 := NewTestProv(30, 30)

	key := types.NewKey("foo.bar.baz")
	repo.Register(key, prov1)
	repo.Register(key, prov2)
	repo.Register(key, prov3)

	tests := []struct {
		key types.Key
		ok  bool
		val types.Value
	}{
		{
			key: types.NewKey("foo"),
			ok:  true,
			val: map[string]types.Value{
				"bar": map[string]types.Value{
					"baz": 30,
				},
			},
		},
		{
			key: types.NewKey("foo.bar"),
			ok:  true,
			val: map[string]types.Value{"baz": 30},
		},
		{
			key: types.NewKey("foo.bar.baz"),
			ok:  true,
			val: 30,
		},
		{
			key: types.NewKey("foo.bar.baz.boo"),
			ok:  false,
			val: nil,
		},
	}

	for _, testCase := range tests {
		val, ok := repo.Get(testCase.key)
		if ok != testCase.ok {
			t.Fatalf("Unexpected key %q lookup result: want %t, got: %t", testCase.key, testCase.ok, ok)
		}

		if !reflect.DeepEqual(val, testCase.val) {
			t.Fatalf("Unexpected value for key %q: want %#v, got %#v", testCase.key, testCase.val, val)
		}
	}
}

func TestTrioProviderThreeKeys(t *testing.T) {
	repo := NewRepository()
	prov1 := NewTestProv(10, 10)
	prov2 := NewTestProv(20, 20)
	prov3 := NewTestProv(30, 30)

	key1 := types.NewKey("k1.k1.k1")
	key2 := types.NewKey("k2.k2.k2")
	key3 := types.NewKey("k3.k3.k3")
	repo.Register(key1, prov1)
	repo.Register(key2, prov2)
	repo.Register(key3, prov3)

	tests := []struct {
		key types.Key
		ok  bool
		val types.Value
	}{
		{
			key: types.NewKey("k1.k1.k1"),
			ok:  true,
			val: prov1.val,
		},
		{
			key: types.NewKey("k2.k2.k2"),
			ok:  true,
			val: prov2.val,
		},
		{
			key: types.NewKey("k3.k3.k3"),
			ok:  true,
			val: prov3.val,
		},
		{
			key: types.NewKey(""),
			ok:  false,
			val: nil,
		},
		{
			key: types.NewKey("k1.k2.k3"),
			ok:  false,
			val: nil,
		},
		{
			key: types.NewKey("k1"),
			ok:  true,
			val: map[string]types.Value{
				"k1": map[string]types.Value{
					"k1": prov1.val,
				},
			},
		},
		{
			key: types.NewKey("k2.k2"),
			ok:  true,
			val: map[string]types.Value{"k2": prov2.val},
		},
		{
			key: types.NewKey("k3.k3.k3.k3"),
			ok:  false,
			val: nil,
		},
	}

	for _, testCase := range tests {
		val, ok := repo.Get(testCase.key)
		if ok != testCase.ok {
			t.Fatalf("Unexpected key %q lookup result: want %t, got: %t", testCase.key, testCase.ok, ok)
		}

		if !reflect.DeepEqual(val, testCase.val) {
			t.Fatalf("Unexpected value for key %q: want %v, got %v", testCase.key, testCase.val, val)
		}
	}
}

func TestTrioProviderNestingKey(t *testing.T) {
	repo := NewRepository()
	prov1 := NewTestProv(10, 10)
	prov2 := NewTestProv(20, 20)
	prov3 := NewTestProv(30, 30)

	key1 := types.NewKey("foo")
	key2 := types.NewKey("foo.bar")
	key3 := types.NewKey("foo.bar.baz")
	repo.Register(key1, prov1)
	repo.Register(key2, prov2)
	repo.Register(key3, prov3)

	tests := []struct {
		key types.Key
		ok  bool
		val types.Value
	}{
		{
			key: key1,
			ok:  true,
			val: prov1.val,
		},
		{
			key: key2,
			ok:  true,
			val: prov2.val,
		},
		{
			key: key3,
			ok:  true,
			val: prov3.val,
		},
		{
			key: types.NewKey(""),
			ok:  false,
			val: nil,
		},
		{
			key: types.NewKey("foo.bar.baz.boo"),
			ok:  false,
			val: nil,
		},
	}

	for _, testCase := range tests {
		val, ok := repo.Get(testCase.key)
		if ok != testCase.ok {
			t.Fatalf("Unexpected key %q lookup result: want %t, got: %t", testCase.key, testCase.ok, ok)
		}

		if !reflect.DeepEqual(val, testCase.val) {
			t.Fatalf("Unexpected value for key %q: want %v, got %v", testCase.key, testCase.val, val)
		}
	}
}

func Test_getAll(t *testing.T) {
	repo := NewRepository()
	n := &node{
		children: map[string]*node{
			"foo": &node{
				children: map[string]*node{
					"baz": &node{
						providers: []Provider{
							NewTestProv(10, 10),
							NewTestProv(5, 5),
						},
					},
				},
			},
			"bar": &node{
				providers: []Provider{
					NewTestProv(20, 20),
				},
			},
		},
	}
	want := map[string]types.Value{
		"foo": map[string]types.Value{
			"baz": 10,
		},
		"bar": 20,
	}
	got := n.getAll(repo, nil).Value
	if !reflect.DeepEqual(want, got) {
		t.Fatalf("Unexpcted traversal value: want: %#v, got: %#v", want, got)
	}
}

type admincfg struct {
	enabled bool
}

type systemcfg struct {
	maxproc  int
	admincfg *admincfg
}

type admincfgmapper struct{}

var _ cast.Mapper = (*admincfgmapper)(nil)

func (acm *admincfgmapper) Map(kv *types.KeyValue) (*types.KeyValue, error) {
	if vmap, ok := kv.Value.(map[string]types.Value); ok {
		res := &admincfg{}
		if enabled, ok := vmap["enabled"]; ok {
			res.enabled = enabled.(bool)
		}
		return &types.KeyValue{kv.Key, res}, nil
	}
	return nil, fmt.Errorf("Conversion to admincfg failed for key: %q value: %#v", kv.Key.String(), kv.Value)
}

type systemcfgmapper struct{}

var _ cast.Mapper = (*systemcfgmapper)(nil)

func (scm *systemcfgmapper) Map(kv *types.KeyValue) (*types.KeyValue, error) {
	if vmap, ok := kv.Value.(map[string]types.Value); ok {
		res := &systemcfg{}
		if ac, ok := vmap["admin"]; ok {
			if acptr, ok := ac.(*admincfg); ok {
				res.admincfg = acptr
			} else {
				return nil, fmt.Errorf("Wrong format for admincfg value: %#v", ac)
			}
		}
		if maxproc, ok := vmap["maxproc"]; ok {
			res.maxproc = maxproc.(int)
		}
		return &types.KeyValue{kv.Key, res}, nil
	}
	return nil, fmt.Errorf("Conversion to systemcfg failed for key: %q value: %#v", kv.Key.String(), kv.Value)
}

func Test_DefineSchema_Primitive(t *testing.T) {
	repoSchema := cast.Schema(map[string]cast.Schema{
		"system": map[string]cast.Schema{
			"__self__": nil,
			"maxproc":  cast.ToInt,
			"admin": map[string]cast.Schema{
				"__self__": nil,
				"enabled":  cast.ToBool,
			},
		},
	})

	tests := []struct {
		name     string
		input    map[string]types.Value
		expected map[string]types.Value
	}{
		{
			name: "No casting",
			input: map[string]types.Value{
				"system.maxproc":       4,
				"system.admin.enabled": true,
			},
			expected: map[string]types.Value{
				"system.maxproc":       4,
				"system.admin.enabled": true,
				"system.admin": map[string]types.Value{
					"enabled": true,
				},
				"system": map[string]types.Value{
					"maxproc": 4,
					"admin": map[string]types.Value{
						"enabled": true,
					},
				},
			},
		},
		{
			name: "Casting from all-strings",
			input: map[string]types.Value{
				"system.maxproc":       "4",
				"system.admin.enabled": "true",
			},
			expected: map[string]types.Value{
				"system.maxproc":       4,
				"system.admin.enabled": true,
				"system.admin": map[string]types.Value{
					"enabled": true,
				},
				"system": map[string]types.Value{
					"maxproc": 4,
					"admin": map[string]types.Value{
						"enabled": true,
					},
				},
			},
		},
		{
			name: "Casting from ptrs",
			input: map[string]types.Value{
				"system.maxproc":       intptr(4),
				"system.admin.enabled": boolptr(true),
			},
			expected: map[string]types.Value{
				"system.maxproc":       4,
				"system.admin.enabled": true,
				"system.admin": map[string]types.Value{
					"enabled": true,
				},
				"system": map[string]types.Value{
					"maxproc": 4,
					"admin": map[string]types.Value{
						"enabled": true,
					},
				},
			},
		},
	}

	t.Parallel()

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			repo := NewRepository()
			repo.DefineSchema(repoSchema)

			for path, value := range testCase.input {
				repo.Register(types.NewKey(path), NewTestProv(value, DefaultWeight))
			}

			for lookupPath, expVal := range testCase.expected {
				gotVal, gotOk := repo.Get(types.NewKey(lookupPath))
				if !gotOk {
					t.Fatalf("Expected lookup for key %q to find a value, none returned", lookupPath)
				}
				if !reflect.DeepEqual(gotVal, expVal) {
					t.Fatalf("Unexpected value returned by lookup for key %q: got: %#v, want: %#v", lookupPath, gotVal, expVal)
				}
			}
		})
	}
}

func Test_DefineSchema_Struct(t *testing.T) {
	repoSchema := cast.Schema(map[string]cast.Schema{
		"system": map[string]cast.Schema{
			"__self__": &systemcfgmapper{},
			"maxproc":  cast.ToInt,
			"admin": map[string]cast.Schema{
				"__self__": &admincfgmapper{},
				"enabled":  cast.ToBool,
			},
		},
	})

	tests := []struct {
		name     string
		input    map[string]types.Value
		expected map[string]types.Value
	}{
		{
			name: "No casting",
			input: map[string]types.Value{
				"system.maxproc":       4,
				"system.admin.enabled": true,
			},
			expected: map[string]types.Value{
				"system.maxproc":       4,
				"system.admin.enabled": true,
				"system.admin":         &admincfg{enabled: true},
				"system":               &systemcfg{admincfg: &admincfg{enabled: true}, maxproc: 4},
			},
		},
		{
			name: "Casting from all-strings",
			input: map[string]types.Value{
				"system.maxproc":       "4",
				"system.admin.enabled": "true",
			},
			expected: map[string]types.Value{
				"system.maxproc":       4,
				"system.admin.enabled": true,
				"system.admin":         &admincfg{enabled: true},
				"system":               &systemcfg{admincfg: &admincfg{enabled: true}, maxproc: 4},
			},
		},
		{
			name: "Casting from ptrs",
			input: map[string]types.Value{
				"system.maxproc":       intptr(4),
				"system.admin.enabled": boolptr(true),
			},
			expected: map[string]types.Value{
				"system.maxproc":       4,
				"system.admin.enabled": true,
				"system.admin":         &admincfg{enabled: true},
				"system":               &systemcfg{admincfg: &admincfg{enabled: true}, maxproc: 4},
			},
		},
	}

	t.Parallel()

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			repo := NewRepository()
			repo.DefineSchema(repoSchema)

			for path, value := range testCase.input {
				repo.Register(types.NewKey(path), NewTestProv(value, DefaultWeight))
			}

			for lookupPath, expVal := range testCase.expected {
				gotVal, gotOk := repo.Get(types.NewKey(lookupPath))
				if !gotOk {
					t.Fatalf("Expected lookup for key %q to find a value, none returned", lookupPath)
				}
				if !reflect.DeepEqual(gotVal, expVal) {
					t.Fatalf("Unexpected value returned by lookup for key %q: got: %#v, want: %#v", lookupPath, gotVal, expVal)
				}
			}
		})
	}
}
