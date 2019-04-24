package cfg

import (
	"reflect"
	"strconv"
	"testing"
)

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
			t.Fatalf("Unexpected key %s lookup result: want %t, got: %t", testCase.key, testCase.ok, ok)
		}
		if !ok {
			continue
		}
		if !reflect.DeepEqual(val, testCase.val) {
			t.Fatalf("Unexpected value for key %s: want %v, got %v", testCase.key, testCase.val, val)
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
			t.Fatalf("Unexpected key %s lookup result: want %t, got: %t", testCase.key, testCase.ok, ok)
		}

		if !reflect.DeepEqual(val, testCase.val) {
			t.Fatalf("Unexpected value for key %s: want %#v, got %#v", testCase.key, testCase.val, val)
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
			t.Fatalf("Unexpected key %s lookup result: want %t, got: %t", testCase.key, testCase.ok, ok)
		}

		if !reflect.DeepEqual(val, testCase.val) {
			t.Fatalf("Unexpected value for key %s: want %v, got %v", testCase.key, testCase.val, val)
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
			t.Fatalf("Unexpected key %s lookup result: want %t, got: %t", testCase.key, testCase.ok, ok)
		}

		if !reflect.DeepEqual(val, testCase.val) {
			t.Fatalf("Unexpected value for key %s: want %v, got %v", testCase.key, testCase.val, val)
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
	got := n.getAll(nil)
	if !reflect.DeepEqual(want, got) {
		t.Fatalf("Unexpcted traversal value: want: %#v, got: %#v", want, got)
	}
}

type TestMapper struct {
	conv func(kv *KeyValue) *KeyValue
}

func NewTestMapper(conv func(kv *KeyValue) *KeyValue) *TestMapper {
	return &TestMapper{
		conv: conv,
	}
}

func (tm *TestMapper) Map(kv *KeyValue) *KeyValue {
	return tm.conv(kv)
}

func Test_DefineMapper(t *testing.T) {
	// Make sure it's clean
	mappers = make(map[string]Mapper)
	mpr := NewTestMapper(func(kv *KeyValue) *KeyValue {
		v := kv.Value
		if _, ok := v.(string); ok {
			convVal, err := strconv.Atoi(v.(string))
			if err != nil {
				t.Fatalf("Failed to convert value to int: %s", err)
			}
			return &KeyValue{kv.Key, convVal}
		}
		return kv
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
