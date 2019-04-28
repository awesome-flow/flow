package cast

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/awesome-flow/flow/pkg/types"
)

func Test_IdentityConverter(t *testing.T) {
	tests := []struct {
		inVal   interface{}
		outVal  interface{}
		outFlag bool
	}{
		{1, 1, true},
		{nil, nil, true},
		{'a', 'a', true},
		{"asdf", "asdf", true},
		{struct{}{}, struct{}{}, true},
	}

	for ix, testCase := range tests {
		t.Run(fmt.Sprintf("Test #%d", ix), func(t *testing.T) {
			in := &types.KeyValue{nil, testCase.inVal}
			out, ok := Identity.Convert(in)
			if ok != testCase.outFlag {
				t.Errorf("Unexpected Convert flag: want: %t, got: %t", testCase.outFlag, ok)
			}
			if !ok {
				return
			}
			if out == nil && testCase.outVal != nil {
				t.Errorf("Expected a non-nil result, got nil")
			}
			if !reflect.DeepEqual(testCase.outVal, out.Value) {
				t.Errorf("Unexpected Convert value: want: %#v, got: %#v", testCase.outVal, out.Value)
			}
		})
	}
}

func Test_IntToStrConverter(t *testing.T) {
	tests := []struct {
		inVal   interface{}
		outVal  interface{}
		outFlag bool
	}{
		{1, "1", true},
		{-1, "-1", true},
		{nil, nil, false},
		{'a', nil, false},
		{"asdf", nil, false},
		{struct{}{}, nil, false},
	}

	for ix, testCase := range tests {
		t.Run(fmt.Sprintf("Test #%d", ix), func(t *testing.T) {
			in := &types.KeyValue{nil, testCase.inVal}
			out, ok := IntToStr.Convert(in)
			if ok != testCase.outFlag {
				t.Errorf("Unexpected Convert flag: want: %t, got: %t", testCase.outFlag, ok)
			}
			if !ok {
				return
			}
			if out == nil && testCase.outVal != nil {
				t.Errorf("Expected a non-nil result, got nil")
			}
			if !reflect.DeepEqual(testCase.outVal, out.Value) {
				t.Errorf("Unexpected Convert value: want: %#v, got: %#v", testCase.outVal, out.Value)
			}
		})
	}
}

func intptr(v int) *int { return &v }
func Test_IntPtrToIntConverter(t *testing.T) {
	tests := []struct {
		inVal   interface{}
		outVal  interface{}
		outFlag bool
	}{
		{1, nil, false},
		{intptr(42), 42, true},
		{nil, nil, false},
		{'a', nil, false},
		{"asdf", nil, false},
		{struct{}{}, nil, false},
	}

	for ix, testCase := range tests {
		t.Run(fmt.Sprintf("Test #%d", ix), func(t *testing.T) {
			in := &types.KeyValue{nil, testCase.inVal}
			out, ok := IntPtrToInt.Convert(in)
			if ok != testCase.outFlag {
				t.Errorf("Unexpected Convert flag: want: %t, got: %t", testCase.outFlag, ok)
			}
			if !ok {
				return
			}
			if out == nil && testCase.outVal != nil {
				t.Errorf("Expected a non-nil result, got nil")
			}
			if !reflect.DeepEqual(testCase.outVal, out.Value) {
				t.Errorf("Unexpected Convert value: want: %#v, got: %#v", testCase.outVal, out.Value)
			}
		})
	}
}

func strptr(v string) *string { return &v }
func Test_StrPtrToStrConverter(t *testing.T) {
	tests := []struct {
		inVal   interface{}
		outVal  interface{}
		outFlag bool
	}{
		{1, nil, false},
		{nil, nil, false},
		{'a', nil, false},
		{"asdf", nil, false},
		{strptr("asdf"), "asdf", true},
		{struct{}{}, nil, false},
	}

	for ix, testCase := range tests {
		t.Run(fmt.Sprintf("Test #%d", ix), func(t *testing.T) {
			in := &types.KeyValue{nil, testCase.inVal}
			out, ok := StrPtrToStr.Convert(in)
			if ok != testCase.outFlag {
				t.Errorf("Unexpected Convert flag: want: %t, got: %t", testCase.outFlag, ok)
			}
			if !ok {
				return
			}
			if out == nil && testCase.outVal != nil {
				t.Errorf("Expected a non-nil result, got nil")
			}
			if !reflect.DeepEqual(testCase.outVal, out.Value) {
				t.Errorf("Unexpected Convert value: want: %#v, got: %#v", testCase.outVal, out.Value)
			}
		})
	}
}

func boolptr(v bool) *bool { return &v }
func Test_BoolPtrToBoolConverter(t *testing.T) {
	tests := []struct {
		inVal   interface{}
		outVal  interface{}
		outFlag bool
	}{
		{true, nil, false},
		{boolptr(true), true, true},
		{boolptr(false), false, true},
		{nil, nil, false},
	}

	for ix, testCase := range tests {
		t.Run(fmt.Sprintf("Test #%d", ix), func(t *testing.T) {
			in := &types.KeyValue{nil, testCase.inVal}
			out, ok := BoolPtrToBool.Convert(in)
			if ok != testCase.outFlag {
				t.Errorf("Unexpected Convert flag: want: %t, got: %t", testCase.outFlag, ok)
			}
			if !ok {
				return
			}
			if out == nil && testCase.outVal != nil {
				t.Errorf("Expected a non-nil result, got nil")
			}
			if !reflect.DeepEqual(testCase.outVal, out.Value) {
				t.Errorf("Unexpected Convert value: want: %#v, got: %#v", testCase.outVal, out.Value)
			}
		})
	}
}

func Test_StrToIntConverter(t *testing.T) {
	tests := []struct {
		inVal   interface{}
		outVal  interface{}
		outFlag bool
	}{
		{1, nil, false},
		{"1", 1, true},
		{"-1", -1, true},
		{"1234567890", 1234567890, true},
		{"asdf", nil, false},
		{'1', nil, false},
	}

	for ix, testCase := range tests {
		t.Run(fmt.Sprintf("Test #%d", ix), func(t *testing.T) {
			in := &types.KeyValue{nil, testCase.inVal}
			out, ok := StrToInt.Convert(in)
			if ok != testCase.outFlag {
				t.Errorf("Unexpected Convert flag: want: %t, got: %t", testCase.outFlag, ok)
			}
			if !ok {
				return
			}
			if out == nil && testCase.outVal != nil {
				t.Errorf("Expected a non-nil result, got nil")
			}
			if !reflect.DeepEqual(testCase.outVal, out.Value) {
				t.Errorf("Unexpected Convert value: want: %#v, got: %#v", testCase.outVal, out.Value)
			}
		})
	}
}

func Test_IfIntConverter(t *testing.T) {
	tests := []struct {
		inVal   interface{}
		outVal  interface{}
		outFlag bool
	}{
		{1, 1, true},
		{-1, -1, true},
		{0, 0, true},
		{"asdf", nil, false},
		{intptr(1), nil, false},
		{"1", nil, false},
		{nil, nil, false},
	}

	for ix, testCase := range tests {
		t.Run(fmt.Sprintf("Test #%d", ix), func(t *testing.T) {
			in := &types.KeyValue{nil, testCase.inVal}
			out, ok := IfInt.Convert(in)
			if ok != testCase.outFlag {
				t.Errorf("Unexpected Convert flag: want: %t, got: %t", testCase.outFlag, ok)
			}
			if !ok {
				return
			}
			if out == nil && testCase.outVal != nil {
				t.Errorf("Expected a non-nil result, got nil")
			}
			if !reflect.DeepEqual(testCase.outVal, out.Value) {
				t.Errorf("Unexpected Convert value: want: %#v, got: %#v", testCase.outVal, out.Value)
			}
		})
	}
}

func Test_IfStrConverter(t *testing.T) {
	tests := []struct {
		inVal   interface{}
		outVal  interface{}
		outFlag bool
	}{
		{1, nil, false},
		{"asdf", "asdf", true},
		{strptr("asdf"), nil, false},
		{'a', nil, false},
		{nil, nil, false},
	}

	for ix, testCase := range tests {
		t.Run(fmt.Sprintf("Test #%d", ix), func(t *testing.T) {
			in := &types.KeyValue{nil, testCase.inVal}
			out, ok := IfStr.Convert(in)
			if ok != testCase.outFlag {
				t.Errorf("Unexpected Convert flag: want: %t, got: %t", testCase.outFlag, ok)
			}
			if !ok {
				return
			}
			if out == nil && testCase.outVal != nil {
				t.Errorf("Expected a non-nil result, got nil")
			}
			if !reflect.DeepEqual(testCase.outVal, out.Value) {
				t.Errorf("Unexpected Convert value: want: %#v, got: %#v", testCase.outVal, out.Value)
			}
		})
	}
}

func Test_IfBoolConverter(t *testing.T) {
	tests := []struct {
		inVal   interface{}
		outVal  interface{}
		outFlag bool
	}{
		{true, true, true},
		{false, false, true},
		{nil, nil, false},
		{"true", nil, false},
		{1, nil, false},
		{0, nil, false},
	}

	for ix, testCase := range tests {
		t.Run(fmt.Sprintf("Test #%d", ix), func(t *testing.T) {
			in := &types.KeyValue{nil, testCase.inVal}
			out, ok := IfBool.Convert(in)
			if ok != testCase.outFlag {
				t.Errorf("Unexpected Convert flag: want: %t, got: %t", testCase.outFlag, ok)
			}
			if !ok {
				return
			}
			if out == nil && testCase.outVal != nil {
				t.Errorf("Expected a non-nil result, got nil")
			}
			if !reflect.DeepEqual(testCase.outVal, out.Value) {
				t.Errorf("Unexpected Convert value: want: %#v, got: %#v", testCase.outVal, out.Value)
			}
		})
	}
}

type convAct struct {
	res types.Value
	ok  bool
}

type testConverter struct {
	conv func(kv *types.KeyValue) (*types.KeyValue, bool)
}

var _ Converter = (*testConverter)(nil)

func NewTestConverter(act convAct) *testConverter {
	return &testConverter{
		conv: func(kv *types.KeyValue) (*types.KeyValue, bool) {
			if act.ok {
				return &types.KeyValue{kv.Key, act.res}, act.ok
			}
			return nil, false
		},
	}
}

func (tc *testConverter) Convert(kv *types.KeyValue) (*types.KeyValue, bool) {
	return tc.conv(kv)
}

func Test_CompositeConverter_CompAnd(t *testing.T) {
	tests := []struct {
		name   string
		chain  []convAct
		expVal types.Value
		expOk  bool
	}{
		{
			"Empty chain",
			[]convAct{},
			nil, false,
		},
		{
			"1 positive",
			[]convAct{
				convAct{1, true},
			},
			1, true,
		},
		{
			"2 positive",
			[]convAct{
				convAct{1, true},
				convAct{2, true},
			},
			2, true,
		},
		{
			"1 negative",
			[]convAct{
				convAct{nil, false},
			},
			nil, false,
		},
		{
			"1 positive 1 negative",
			[]convAct{
				convAct{1, true},
				convAct{nil, false},
			},
			nil, false,
		},
		{
			"1 negative 1 positive",
			[]convAct{
				convAct{nil, false},
				convAct{1, true},
			},
			nil, false,
		},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			convChain := make([]Converter, 0, len(testCase.chain))
			for _, act := range testCase.chain {
				convChain = append(convChain, NewTestConverter(act))
			}

			comp := NewCompositeConverter(CompAnd, convChain...)
			// None of the converters react to the input kv, so
			// passing a nil value
			got, gotOk := comp.Convert(&types.KeyValue{nil, nil})
			if gotOk != testCase.expOk {
				t.Errorf("Unexpected Convert flag: want: %t, got: %t", testCase.expOk, gotOk)
			}
			if !gotOk {
				return
			}
			if !reflect.DeepEqual(testCase.expVal, got.Value) {
				t.Errorf("Unexpected Convert value: want: %#v, got: %#v", testCase.expVal, got.Value)
			}
		})
	}
}

func Test_CompositeConverter_CompOr(t *testing.T) {
	tests := []struct {
		name   string
		chain  []convAct
		expVal types.Value
		expOk  bool
	}{
		{
			"Empty chain",
			[]convAct{},
			nil, false,
		},
		{
			"1 positive",
			[]convAct{
				convAct{1, true},
			},
			1, true,
		},
		{
			"2 positive",
			[]convAct{
				convAct{1, true},
				convAct{2, true},
			},
			1, true,
		},
		{
			"1 negative",
			[]convAct{
				convAct{nil, false},
			},
			nil, false,
		},
		{
			"1 positive 1 negative",
			[]convAct{
				convAct{1, true},
				convAct{nil, false},
			},
			1, true,
		},
		{
			"1 negative 1 positive",
			[]convAct{
				convAct{nil, false},
				convAct{2, true},
			},
			2, true,
		},
		{
			"1 negative 2 positives",
			[]convAct{
				convAct{nil, false},
				convAct{1, true},
				convAct{2, true},
			},
			1, true,
		},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			convChain := make([]Converter, 0, len(testCase.chain))
			for _, act := range testCase.chain {
				convChain = append(convChain, NewTestConverter(act))
			}

			comp := NewCompositeConverter(CompOr, convChain...)
			// None of the converters react to the input kv, so
			// passing a nil value
			got, gotOk := comp.Convert(&types.KeyValue{nil, nil})
			if gotOk != testCase.expOk {
				t.Errorf("Unexpected Convert flag: want: %t, got: %t", testCase.expOk, gotOk)
			}
			if !gotOk {
				return
			}
			if !reflect.DeepEqual(testCase.expVal, got.Value) {
				t.Errorf("Unexpected Convert value: want: %#v, got: %#v", testCase.expVal, got.Value)
			}
		})
	}
}

func Test_CompositeConverter_CompFirst(t *testing.T) {
	tests := []struct {
		name   string
		chain  []convAct
		expVal types.Value
		expOk  bool
	}{
		{
			"Empty chain",
			[]convAct{},
			nil, false,
		},
		{
			"1 positive",
			[]convAct{
				convAct{1, true},
			},
			1, true,
		},
		{
			"2 positive",
			[]convAct{
				convAct{1, true},
				convAct{2, true},
			},
			1, true,
		},
		{
			"1 negative",
			[]convAct{
				convAct{nil, false},
			},
			nil, false,
		},
		{
			"1 positive 1 negative",
			[]convAct{
				convAct{1, true},
				convAct{nil, false},
			},
			1, true,
		},
		{
			"1 negative 1 positive",
			[]convAct{
				convAct{nil, false},
				convAct{2, true},
			},
			2, true,
		},
		{
			"1 negative 2 positives",
			[]convAct{
				convAct{nil, false},
				convAct{1, true},
				convAct{2, true},
			},
			1, true,
		},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			convChain := make([]Converter, 0, len(testCase.chain))
			for _, act := range testCase.chain {
				convChain = append(convChain, NewTestConverter(act))
			}

			comp := NewCompositeConverter(CompFirst, convChain...)
			// None of the converters react to the input kv, so
			// passing a nil value
			got, gotOk := comp.Convert(&types.KeyValue{nil, nil})
			if gotOk != testCase.expOk {
				t.Errorf("Unexpected Convert flag: want: %t, got: %t", testCase.expOk, gotOk)
			}
			if !gotOk {
				return
			}
			if !reflect.DeepEqual(testCase.expVal, got.Value) {
				t.Errorf("Unexpected Convert value: want: %#v, got: %#v", testCase.expVal, got.Value)
			}
		})
	}
}

func Test_CompositeConverter_CompLast(t *testing.T) {
	tests := []struct {
		name   string
		chain  []convAct
		expVal types.Value
		expOk  bool
	}{
		{
			"Empty chain",
			[]convAct{},
			nil, false,
		},
		{
			"1 positive",
			[]convAct{
				convAct{1, true},
			},
			1, true,
		},
		{
			"2 positive",
			[]convAct{
				convAct{1, true},
				convAct{2, true},
			},
			2, true,
		},
		{
			"1 negative",
			[]convAct{
				convAct{nil, false},
			},
			nil, false,
		},
		{
			"1 positive 1 negative",
			[]convAct{
				convAct{1, true},
				convAct{nil, false},
			},
			1, true,
		},
		{
			"1 negative 1 positive",
			[]convAct{
				convAct{nil, false},
				convAct{2, true},
			},
			2, true,
		},
		{
			"1 negative 2 positives",
			[]convAct{
				convAct{nil, false},
				convAct{1, true},
				convAct{2, true},
			},
			2, true,
		},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			convChain := make([]Converter, 0, len(testCase.chain))
			for _, act := range testCase.chain {
				convChain = append(convChain, NewTestConverter(act))
			}

			comp := NewCompositeConverter(CompLast, convChain...)
			// None of the converters react to the input kv, so
			// passing a nil value
			got, gotOk := comp.Convert(&types.KeyValue{nil, nil})
			if gotOk != testCase.expOk {
				t.Errorf("Unexpected Convert flag: want: %t, got: %t", testCase.expOk, gotOk)
			}
			if !gotOk {
				return
			}
			if !reflect.DeepEqual(testCase.expVal, got.Value) {
				t.Errorf("Unexpected Convert value: want: %#v, got: %#v", testCase.expVal, got.Value)
			}
		})
	}
}
