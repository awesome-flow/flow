package cast

import (
	"reflect"
	"testing"
)

type TestMapper struct {
	conv func(kv *KeyValue) (*KeyValue, error)
}

func NewTestMapper(conv func(kv *KeyValue) (*KeyValue, error)) *TestMapper {
	return &TestMapper{
		conv: conv,
	}
}

func (tm *TestMapper) Map(kv *KeyValue) (*KeyValue, error) {
	return tm.conv(kv)
}

func Test_MapperNode_Insert(t *testing.T) {
	mpr := NewTestMapper(func(kv *KeyValue) (*KeyValue, error) {
		return kv, nil
	})
	tests := []struct {
		path string
		exp  *MapperNode
	}{

		{
			"",
			&MapperNode{
				Children: map[string]*MapperNode{},
			},
		},
		{
			"foo",
			&MapperNode{
				Children: map[string]*MapperNode{
					"foo": &MapperNode{
						Mpr:      mpr,
						Children: map[string]*MapperNode{},
					},
				},
			},
		},
		{
			"foo.bar",
			&MapperNode{
				Children: map[string]*MapperNode{
					"foo": &MapperNode{
						Children: map[string]*MapperNode{
							"bar": &MapperNode{
								Mpr:      mpr,
								Children: map[string]*MapperNode{},
							},
						},
					},
				},
			},
		},
		{
			"foo.*.bar",
			&MapperNode{
				Children: map[string]*MapperNode{
					"foo": &MapperNode{
						Children: map[string]*MapperNode{
							"*": &MapperNode{
								Children: map[string]*MapperNode{
									"bar": &MapperNode{
										Mpr:      mpr,
										Children: map[string]*MapperNode{},
									},
								},
							},
						},
					},
				},
			},
		},
	}

	t.Parallel()

	for _, testCase := range tests {
		t.Run(testCase.path, func(t *testing.T) {
			root := NewMapperNode()
			root.Insert(NewKey(testCase.path), mpr)
			if !reflect.DeepEqual(testCase.exp, root) {
				t.Errorf("Unexpected node structure: want: %#v, got: %#v", testCase.exp, root)
			}
		})
	}
}

func Test_MapperNode_Find_SingleEntryLookup(t *testing.T) {
	tests := []struct {
		insertPaths []string
		lookupPath  string
	}{
		{
			[]string{"foo", "*"},
			"foo",
		},
		{
			[]string{"foo.bar", "foo.*", "*.bar", "*.*"},
			"foo.bar",
		},
		{
			[]string{"foo.bar.baz", "foo.bar.*", "foo.*.baz", "foo.*.*", "*.bar.baz", "*.bar.*", "*.*.baz", "*.*.*"},
			"foo.bar.baz",
		},
	}

	t.Parallel()

	for _, testCase := range tests {
		for _, insertPath := range testCase.insertPaths {
			t.Run(insertPath, func(t *testing.T) {
				mpr := NewTestMapper(func(kv *KeyValue) (*KeyValue, error) { return kv, nil })
				root := NewMapperNode()
				root.Insert(NewKey(insertPath), mpr)
				v := root.Find(NewKey(testCase.lookupPath))
				if v == nil {
					t.Fatalf("Expected to get a lookup result for key %q, got nil", testCase.lookupPath)
				}
				if v.Mpr != mpr {
					t.Fatalf("Unexpected mapper value returned by the key %q lookup: %#v, want: %#v", testCase.lookupPath, v.Mpr, mpr)
				}
			})
		}
	}
}

func Test_MapperNode_Find_Precedence(t *testing.T) {
	convFunc := func(kv *KeyValue) (*KeyValue, error) { return kv, nil }
	mprAstrx, mprExct := NewTestMapper(convFunc), NewTestMapper(convFunc)

	tests := []struct {
		exactPath  string
		astrxPaths []string
	}{
		{
			"foo",
			[]string{"*"},
		},
		{
			"foo.bar",
			[]string{"foo.*", "*.bar", "*.*"},
		},
		{
			"foo.bar.baz",
			[]string{"foo.bar.*", "foo.*.baz", "foo.*.*", "*.bar.baz", "*.bar.*", "*.*.baz", "*.*.*"},
		},
	}

	for _, testCase := range tests {
		t.Run(testCase.exactPath, func(t *testing.T) {
			root := NewMapperNode()
			root.Insert(NewKey(testCase.exactPath), mprExct)
			for _, astrxPath := range testCase.astrxPaths {
				root.Insert(NewKey(astrxPath), mprAstrx)
			}
			v := root.Find(NewKey(testCase.exactPath))
			if v == nil {
				t.Fatalf("Expected to get a non-nil lookup result for key %q, git nil", testCase.exactPath)
			}
			if v.Mpr != mprExct {
				t.Fatalf("Unexpected value returned by the key %q lookup: got: %#v, want: %#v", testCase.exactPath, v.Mpr, mprExct)
			}
		})
	}
}

func Test_ConvMapper(t *testing.T) {
	tests := []struct {
		name      string
		conv      Converter
		expVal    Value
		validIn   []Value
		invalidIn []Value
	}{
		{
			name:      "conversion to Int",
			conv:      ToInt,
			expVal:    42,
			validIn:   []Value{42, "42", intptr(42)},
			invalidIn: []Value{true, "", '0', nil},
		},
		{
			name:      "conversion to Str",
			conv:      ToStr,
			expVal:    "42",
			validIn:   []Value{"42", 42, strptr("42")},
			invalidIn: []Value{intptr(42), nil, false, '0'},
		},
		{
			name:      "conversion to Bool",
			conv:      ToBool,
			expVal:    true,
			validIn:   []Value{true, boolptr(true), "true", "y", 1, "1"},
			invalidIn: []Value{123, "asdf", nil},
		},
	}

	t.Parallel()

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			mpr := NewConvMapper(testCase.conv)
			for _, val := range testCase.validIn {
				conv, convErr := mpr.Map(&KeyValue{nil, val})
				if convErr != nil {
					t.Fatalf("Unexpected mapping error for input value %#v", val)
				}
				if !reflect.DeepEqual(conv.Value, testCase.expVal) {
					t.Fatalf("Unexpected mapping value for input value %#v: got: %#v, want: %#v", val, conv.Value, testCase.expVal)
				}
			}
			for _, val := range testCase.invalidIn {
				_, convErr := mpr.Map(&KeyValue{nil, val})
				if convErr == nil {
					t.Fatalf("Expected to get an error while converting %#v, got nil", val)
				}
			}
		})
	}
}
