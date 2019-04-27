package cfg

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

func Test_mapperNode_Insert(t *testing.T) {
	mpr := NewTestMapper(func(kv *KeyValue) (*KeyValue, error) {
		return kv, nil
	})
	tests := []struct {
		path string
		exp  *mapperNode
	}{

		{
			"",
			&mapperNode{
				children: map[string]*mapperNode{},
			},
		},
		{
			"foo",
			&mapperNode{
				children: map[string]*mapperNode{
					"foo": &mapperNode{
						mpr:      mpr,
						children: map[string]*mapperNode{},
					},
				},
			},
		},
		{
			"foo.bar",
			&mapperNode{
				children: map[string]*mapperNode{
					"foo": &mapperNode{
						children: map[string]*mapperNode{
							"bar": &mapperNode{
								mpr:      mpr,
								children: map[string]*mapperNode{},
							},
						},
					},
				},
			},
		},
		{
			"foo.*.bar",
			&mapperNode{
				children: map[string]*mapperNode{
					"foo": &mapperNode{
						children: map[string]*mapperNode{
							"*": &mapperNode{
								children: map[string]*mapperNode{
									"bar": &mapperNode{
										mpr:      mpr,
										children: map[string]*mapperNode{},
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
			root := newMapperNode()
			root.Insert(NewKey(testCase.path), mpr)
			if !reflect.DeepEqual(testCase.exp, root) {
				t.Errorf("Unexpected node structure: want: %#v, got: %#v", testCase.exp, root)
			}
		})
	}
}

func Test_mapperNode_Find_SingleEntryLookup(t *testing.T) {
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
				root := newMapperNode()
				root.Insert(NewKey(insertPath), mpr)
				v := root.Find(NewKey(testCase.lookupPath))
				if v == nil {
					t.Fatalf("Expected to get a lookup result for key %q, got nil", testCase.lookupPath)
				}
				if v.mpr != mpr {
					t.Fatalf("Unexpected mapper value returned by the key %q lookup: %#v, want: %#v", testCase.lookupPath, v.mpr, mpr)
				}
			})
		}
	}
}

func Test_mapperNode_Find_Precedence(t *testing.T) {
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
			root := newMapperNode()
			root.Insert(NewKey(testCase.exactPath), mprExct)
			for _, astrxPath := range testCase.astrxPaths {
				root.Insert(NewKey(astrxPath), mprAstrx)
			}
			v := root.Find(NewKey(testCase.exactPath))
			if v == nil {
				t.Fatalf("Expected to get a non-nil lookup result for key %q, git nil", testCase.exactPath)
			}
			if v.mpr != mprExct {
				t.Fatalf("Unexpected value returned by the key %q lookup: got: %#v, want: %#v", testCase.exactPath, v.mpr, mprExct)
			}
		})
	}
}
