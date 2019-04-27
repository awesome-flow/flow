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
