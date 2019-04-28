package cast

import (
	"strconv"

	"github.com/awesome-flow/flow/pkg/types"
)

type Converter interface {
	Convert(kv *types.KeyValue) (*types.KeyValue, bool)
}

type IdentityConverter struct{}

var _ Converter = (*IdentityConverter)(nil)

func (*IdentityConverter) Convert(kv *types.KeyValue) (*types.KeyValue, bool) {
	return kv, true
}

type IntPtrToIntConverter struct{}

var _ Converter = (*IntPtrToIntConverter)(nil)

func (*IntPtrToIntConverter) Convert(kv *types.KeyValue) (*types.KeyValue, bool) {
	if pv, ok := kv.Value.(*int); ok {
		return &types.KeyValue{kv.Key, *pv}, true
	}
	return nil, false
}

type BoolPtrToBoolConverter struct{}

var _ Converter = (*BoolPtrToBoolConverter)(nil)

func (*BoolPtrToBoolConverter) Convert(kv *types.KeyValue) (*types.KeyValue, bool) {
	if pv, ok := kv.Value.(*bool); ok {
		return &types.KeyValue{kv.Key, *pv}, true
	}
	return nil, false
}

type StrPtrToStrConverter struct{}

var _ Converter = (*StrPtrToStrConverter)(nil)

func (*StrPtrToStrConverter) Convert(kv *types.KeyValue) (*types.KeyValue, bool) {
	if spv, ok := kv.Value.(*string); ok {
		return &types.KeyValue{kv.Key, *spv}, true
	}
	return nil, false
}

type StrToBoolConverter struct{}

var _ Converter = (*StrToBoolConverter)(nil)

func (*StrToBoolConverter) Convert(kv *types.KeyValue) (*types.KeyValue, bool) {
	if sv, ok := kv.Value.(string); ok {
		switch sv {
		case "true", "1", "y":
			return &types.KeyValue{kv.Key, true}, true
		case "false", "0", "n":
			return &types.KeyValue{kv.Key, false}, true
		}
	}
	return nil, false
}

type StrToIntConverter struct{}

var _ Converter = (*StrToIntConverter)(nil)

func (*StrToIntConverter) Convert(kv *types.KeyValue) (*types.KeyValue, bool) {
	if sv, ok := kv.Value.(string); ok {
		s, err := strconv.Atoi(sv)
		if err == nil {
			return &types.KeyValue{kv.Key, s}, true
		}
	}
	return nil, false
}

type IntToBoolConverter struct{}

var _ Converter = (*IntToBoolConverter)(nil)

func (*IntToBoolConverter) Convert(kv *types.KeyValue) (*types.KeyValue, bool) {
	if mv, ok := kv.Value.(int); ok {
		var v bool
		if mv == 0 {
			v = false
		} else if mv == 1 {
			v = true
		} else {
			return nil, false
		}
		return &types.KeyValue{kv.Key, v}, true
	}
	return nil, false
}

type IntToStrConverter struct{}

var _ Converter = (*IntToStrConverter)(nil)

func (*IntToStrConverter) Convert(kv *types.KeyValue) (*types.KeyValue, bool) {
	if iv, ok := kv.Value.(int); ok {
		return &types.KeyValue{kv.Key, strconv.Itoa(iv)}, true
	}
	return nil, false
}

type IfIntConverter struct{}

var _ Converter = (*IfIntConverter)(nil)

func (*IfIntConverter) Convert(kv *types.KeyValue) (*types.KeyValue, bool) {
	if _, ok := kv.Value.(int); ok {
		return kv, true
	}
	return nil, false
}

type IfStrConverter struct{}

var _ Converter = (*IfStrConverter)(nil)

func (*IfStrConverter) Convert(kv *types.KeyValue) (*types.KeyValue, bool) {
	if _, ok := kv.Value.(string); ok {
		return kv, true
	}
	return nil, false
}

type IfBoolConverter struct{}

var _ Converter = (*IfBoolConverter)(nil)

func (*IfBoolConverter) Convert(kv *types.KeyValue) (*types.KeyValue, bool) {
	if _, ok := kv.Value.(bool); ok {
		return kv, true
	}
	return nil, false
}

//======== Composite converters =======

type CompositionStrategy uint8

const (
	CompNone CompositionStrategy = iota
	CompAnd
	CompOr
	CompFirst
	CompLast
)

type CompositeConverter struct {
	strategy   CompositionStrategy
	converters []Converter
}

func NewCompositeConverter(strategy CompositionStrategy, convs ...Converter) *CompositeConverter {
	return &CompositeConverter{
		strategy:   strategy,
		converters: convs,
	}
}

func (cc *CompositeConverter) Convert(kv *types.KeyValue) (*types.KeyValue, bool) {
	switch cc.strategy {
	case CompNone:
		return kv, true
	case CompAnd:
		mkv := kv
		var ok bool
		for _, conv := range cc.converters {
			mkv, ok = conv.Convert(mkv)
			if !ok {
				return nil, false
			}
		}
		return mkv, ok
	case CompFirst, CompOr:
		for _, conv := range cc.converters {
			if mkv, ok := conv.Convert(kv); ok {
				return mkv, ok
			}
		}
		return nil, false
	case CompLast:
		var res *types.KeyValue
		var resok bool
		for _, conv := range cc.converters {
			if mkv, ok := conv.Convert(kv); ok {
				res = mkv
				resok = ok
			}
		}
		return res, resok
	}
	return nil, false
}

var (
	Identity *IdentityConverter

	BoolPtrToBool *BoolPtrToBoolConverter
	IntPtrToInt   *IntPtrToIntConverter
	StrPtrToStr   *StrPtrToStrConverter

	IntToBool *IntToBoolConverter
	IntToStr  *IntToStrConverter
	StrToBool *StrToBoolConverter
	StrToInt  *StrToIntConverter

	IfInt  *IfIntConverter
	IfStr  *IfStrConverter
	IfBool *IfBoolConverter

	IntOrIntPtr   *CompositeConverter
	StrOrStrPtr   *CompositeConverter
	BoolOrBoolPtr *CompositeConverter

	ToInt  *CompositeConverter
	ToStr  *CompositeConverter
	ToBool *CompositeConverter
)

func init() {
	IntOrIntPtr = NewCompositeConverter(CompOr, IfInt, IntPtrToInt)
	StrOrStrPtr = NewCompositeConverter(CompOr, IfStr, StrPtrToStr)
	BoolOrBoolPtr = NewCompositeConverter(CompOr, IfBool, BoolPtrToBool)

	ToInt = NewCompositeConverter(CompOr, IntOrIntPtr, StrToInt)
	ToStr = NewCompositeConverter(CompOr, StrOrStrPtr, IntToStr)
	ToBool = NewCompositeConverter(CompOr, BoolOrBoolPtr, StrToBool, IntToBool)
}
