package cfg

import (
	"fmt"
	"reflect"
	"strconv"
	"testing"
)

func flushMappers() {
	mappersMx.Lock()
	defer mappersMx.Unlock()
	mappers = newMapperNode()
}

type TestProv struct {
	val     Value
	weight  int
	isSetUp bool
}

func NewTestProv(val Value, weight int) *TestProv {
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

func (tp *TestProv) Get(key Key) (*KeyValue, bool) {
	return &KeyValue{
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
	key := NewKey("foo.bar.baz")
	repo.Register(key, prov)

	tests := []struct {
		key Key
		ok  bool
		val Value
	}{
		{
			key: NewKey("foo"),
			ok:  true,
			val: map[string]Value{
				"bar": map[string]Value{
					"baz": 42,
				},
			},
		},
		{
			key: NewKey("foo.bar"),
			ok:  true,
			val: map[string]Value{"baz": 42},
		},
		{
			key: NewKey("foo.bar.baz"),
			ok:  true,
			val: 42,
		},
		{
			key: NewKey("foo.bar.baz.boo"),
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

	key := NewKey("foo.bar.baz")
	repo.Register(key, prov1)
	repo.Register(key, prov2)
	repo.Register(key, prov3)

	tests := []struct {
		key Key
		ok  bool
		val Value
	}{
		{
			key: NewKey("foo"),
			ok:  true,
			val: map[string]Value{
				"bar": map[string]Value{
					"baz": 30,
				},
			},
		},
		{
			key: NewKey("foo.bar"),
			ok:  true,
			val: map[string]Value{"baz": 30},
		},
		{
			key: NewKey("foo.bar.baz"),
			ok:  true,
			val: 30,
		},
		{
			key: NewKey("foo.bar.baz.boo"),
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

	key1 := NewKey("k1.k1.k1")
	key2 := NewKey("k2.k2.k2")
	key3 := NewKey("k3.k3.k3")
	repo.Register(key1, prov1)
	repo.Register(key2, prov2)
	repo.Register(key3, prov3)

	tests := []struct {
		key Key
		ok  bool
		val Value
	}{
		{
			key: NewKey("k1.k1.k1"),
			ok:  true,
			val: prov1.val,
		},
		{
			key: NewKey("k2.k2.k2"),
			ok:  true,
			val: prov2.val,
		},
		{
			key: NewKey("k3.k3.k3"),
			ok:  true,
			val: prov3.val,
		},
		{
			key: NewKey(""),
			ok:  false,
			val: nil,
		},
		{
			key: NewKey("k1.k2.k3"),
			ok:  false,
			val: nil,
		},
		{
			key: NewKey("k1"),
			ok:  true,
			val: map[string]Value{
				"k1": map[string]Value{
					"k1": prov1.val,
				},
			},
		},
		{
			key: NewKey("k2.k2"),
			ok:  true,
			val: map[string]Value{"k2": prov2.val},
		},
		{
			key: NewKey("k3.k3.k3.k3"),
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

	key1 := NewKey("foo")
	key2 := NewKey("foo.bar")
	key3 := NewKey("foo.bar.baz")
	repo.Register(key1, prov1)
	repo.Register(key2, prov2)
	repo.Register(key3, prov3)

	tests := []struct {
		key Key
		ok  bool
		val Value
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
			key: NewKey(""),
			ok:  false,
			val: nil,
		},
		{
			key: NewKey("foo.bar.baz.boo"),
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
	want := map[string]Value{
		"foo": map[string]Value{
			"baz": 10,
		},
		"bar": 20,
	}
	got := n.getAll(nil).Value
	if !reflect.DeepEqual(want, got) {
		t.Fatalf("Unexpcted traversal value: want: %#v, got: %#v", want, got)
	}
}

func Test_DefineMapper_simpleConv(t *testing.T) {
	// Make sure it's clean
	flushMappers()
	mpr := NewTestMapper(func(kv *KeyValue) (*KeyValue, error) {
		v := kv.Value
		if _, ok := v.(int); ok {
			return kv, nil
		} else if _, ok := v.(string); ok {
			convVal, err := strconv.Atoi(v.(string))
			if err != nil {
				t.Fatalf("Failed to convert value to int: %s", err)
			}
			return &KeyValue{kv.Key, convVal}, nil
		}
		return nil, fmt.Errorf("unrecognised value type: %#v", kv.Value)
	})
	k := "foo.bar.baz"
	DefineMapper(k, mpr)
	repo := NewRepository()
	prov := NewTestProv("10", 10)
	repo.Register(NewKey(k), prov)
	got, ok := repo.Get(NewKey(k))
	want := 10
	if !ok {
		t.Fatalf("Lookup failed")
	}
	if !reflect.DeepEqual(got, 10) {
		t.Fatalf("Unexpected value: got: %#v, want: %#v", got, want)
	}
}

type Compound struct {
	bar int
	baz string
}

func Test_DefineMapper_nestedConv(t *testing.T) {
	flushMappers()
	fooBarMpr := NewTestMapper(func(kv *KeyValue) (*KeyValue, error) {
		if _, ok := kv.Value.(int); ok {
			return kv, nil
		} else if _, ok := kv.Value.(string); ok {
			if convVal, err := strconv.Atoi(kv.Value.(string)); err == nil {
				return &KeyValue{kv.Key, convVal}, nil
			} else {
				return nil, fmt.Errorf("failed to convert value %#v: %s", kv.Value, err)
			}
		}
		return nil, fmt.Errorf("unrecognised value type: %#v", kv.Value)
	})
	fooBazMpr := NewTestMapper(func(kv *KeyValue) (*KeyValue, error) {
		if _, ok := kv.Value.(string); ok {
			return kv, nil
		} else if _, ok := kv.Value.(int); ok {
			return &KeyValue{kv.Key, strconv.Itoa(kv.Value.(int))}, nil
		}
		return nil, fmt.Errorf("unrecognised value type: %#v", kv.Value)
	})
	fooMpr := NewTestMapper(func(kv *KeyValue) (*KeyValue, error) {
		val := &Compound{}
		if kvMap, ok := kv.Value.(map[string]Value); ok {
			if bar, ok := kvMap["bar"]; ok {
				val.bar = bar.(int)
			}
			if baz, ok := kvMap["baz"]; ok {
				val.baz = baz.(string)
			}
		} else {
			return nil, fmt.Errorf("unrecognised value type: %#v, want: map[string]Value", kv.Value)
		}
		return &KeyValue{kv.Key, val}, nil
	})
	DefineMapper("foo.bar", fooBarMpr)
	DefineMapper("foo.baz", fooBazMpr)
	DefineMapper("foo", fooMpr)

	repo := NewRepository()
	repo.Register(NewKey("foo.bar"), NewTestProv("42", DefaultWeight))
	repo.Register(NewKey("foo.baz"), NewTestProv(123, DefaultWeight))

	if v, ok := repo.Get(NewKey("foo.bar")); !ok {
		t.Fatalf("expected repo to find key foo.bar")
	} else if v != 42 {
		t.Fatalf("unexpected value: got: %#v, want: 42", v)
	}

	if v, ok := repo.Get(NewKey("foo.baz")); !ok {
		t.Fatalf("expected repo to find key foo.baz")
	} else if v != "123" {
		t.Fatalf("unexpected value: got: %#v, want: \"123\"", v)
	}

	expectedComp := &Compound{
		bar: 42,
		baz: "123",
	}
	if v, ok := repo.Get(NewKey("foo")); !ok {
		t.Fatalf("expected repo to find key foo")
	} else if !reflect.DeepEqual(expectedComp, v) {
		t.Fatalf("unexpected value: got: %#v, want: %#v", v, expectedComp)
	}
}

func Test_DefineMap(t *testing.T) {
	repoMap := Map(map[string]Map{
		"system": map[string]Map{
			"maxproc": ToInt,
			"admin": map[string]Map{
				"enabled": ToBool,
			},
		},
	})

	tests := []struct {
		name     string
		input    map[string]Value
		expected map[string]Value
	}{
		{
			name: "No casting",
			input: map[string]Value{
				"system.maxproc":       4,
				"system.admin.enabled": true,
			},
			expected: map[string]Value{
				"system.maxproc":       4,
				"system.admin.enabled": true,
				"system.admin": map[string]Value{
					"enabled": true,
				},
				"system": map[string]Value{
					"maxproc": 4,
					"admin": map[string]Value{
						"enabled": true,
					},
				},
			},
		},
		{
			name: "Primitive casting from all-strings",
			input: map[string]Value{
				"system.maxproc":       "4",
				"system.admin.enabled": "true",
			},
			expected: map[string]Value{
				"system.maxproc":       4,
				"system.admin.enabled": true,
				"system.admin": map[string]Value{
					"enabled": true,
				},
				"system": map[string]Value{
					"maxproc": 4,
					"admin": map[string]Value{
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
			repo.DefineMap(repoMap)

			for path, value := range testCase.input {
				repo.Register(NewKey(path), NewTestProv(value, DefaultWeight))
			}

			for lookupPath, expVal := range testCase.expected {
				gotVal, gotOk := repo.Get(NewKey(lookupPath))
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
