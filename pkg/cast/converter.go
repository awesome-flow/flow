package cast

import (
	"strconv"

	"github.com/awesome-flow/flow/pkg/types"
)

// Converter is a primary interface for converting actors. It represents an
// act of best-effort converstion: either converts or gives up.
type Converter interface {
	// Convert is the function a Converter is expected to define. Returns
	// a converted value and a boolean flag indicating whether the conversion
	// took a place.
	Convert(kv *types.KeyValue) (*types.KeyValue, bool)
}

// IdentityConverter represents an identity function returning the original
// value and a success flag.
type IdentityConverter struct{}

var _ Converter = (*IdentityConverter)(nil)

// Convert returns the kv pair itself and true, no matter what value is provided.
func (*IdentityConverter) Convert(kv *types.KeyValue) (*types.KeyValue, bool) {
	return kv, true
}

// IntPtrToIntConverter performs conversion from an int pointer to int.
type IntPtrToIntConverter struct{}

var _ Converter = (*IntPtrToIntConverter)(nil)

// Convert returns an integer and true if the argument value is a pointer to int.
// Returns nil, false if cast to *int fails.
func (*IntPtrToIntConverter) Convert(kv *types.KeyValue) (*types.KeyValue, bool) {
	if pv, ok := kv.Value.(*int); ok {
		return &types.KeyValue{Key: kv.Key, Value: *pv}, true
	}
	return nil, false
}

// BoolPtrToBoolConverter performs conversion from a boolean pointer to boolean.
type BoolPtrToBoolConverter struct{}

var _ Converter = (*BoolPtrToBoolConverter)(nil)

// Convert returns a bool and true if the argument value is a poiter to bool.
// Returns nil, false if cast to *bool fails.
func (*BoolPtrToBoolConverter) Convert(kv *types.KeyValue) (*types.KeyValue, bool) {
	if pv, ok := kv.Value.(*bool); ok {
		return &types.KeyValue{Key: kv.Key, Value: *pv}, true
	}
	return nil, false
}

// StrPtrToStrConverter performs conversion from a string pointer to string.
type StrPtrToStrConverter struct{}

var _ Converter = (*StrPtrToStrConverter)(nil)

// Convert returns a string and true if the argument value is a pointer to string.
// Returns nil, false if cast to *string fails.
func (*StrPtrToStrConverter) Convert(kv *types.KeyValue) (*types.KeyValue, bool) {
	if spv, ok := kv.Value.(*string); ok {
		return &types.KeyValue{Key: kv.Key, Value: *spv}, true
	}
	return nil, false
}

// StrToBoolConverter performs conventional conversion from a string to a bool value.
type StrToBoolConverter struct{}

var _ Converter = (*StrToBoolConverter)(nil)

// Convert returns true, true for strings "true", "1", "y".
// For strings "false", "0" and "n" returns false, true.
// Returns false, false otherwise treating the case as non-successful conversion.
func (*StrToBoolConverter) Convert(kv *types.KeyValue) (*types.KeyValue, bool) {
	if sv, ok := kv.Value.(string); ok {
		switch sv {
		case "true", "1", "y":
			return &types.KeyValue{Key: kv.Key, Value: true}, true
		case "false", "0", "n":
			return &types.KeyValue{Key: kv.Key, Value: false}, true
		}
	}
	return nil, false
}

// StrToIntConverter performs conventional conversion from a string to int.
type StrToIntConverter struct{}

var _ Converter = (*StrToIntConverter)(nil)

// Convert returns an int, true if the argument value can be parsed with
// strconv.Atoi. Returns nil, false otherwise.
func (*StrToIntConverter) Convert(kv *types.KeyValue) (*types.KeyValue, bool) {
	if sv, ok := kv.Value.(string); ok {
		s, err := strconv.Atoi(sv)
		if err == nil {
			return &types.KeyValue{Key: kv.Key, Value: s}, true
		}
	}
	return nil, false
}

// IntToBoolConverter performs conventional conversion from an int to bool.
type IntToBoolConverter struct{}

var _ Converter = (*IntToBoolConverter)(nil)

// Convert returns false, true for the argument value = 0.
// Returns true, true for the argument value = 1.
// Returns nil, false otherwise.
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
		return &types.KeyValue{Key: kv.Key, Value: v}, true
	}
	return nil, false
}

// IntToStrConverter performs an int to string conversion.
type IntToStrConverter struct{}

var _ Converter = (*IntToStrConverter)(nil)

// Convert returns a string, false if the argument value is an int.
// Returns nil, false otherwise.
func (*IntToStrConverter) Convert(kv *types.KeyValue) (*types.KeyValue, bool) {
	if iv, ok := kv.Value.(int); ok {
		return &types.KeyValue{Key: kv.Key, Value: strconv.Itoa(iv)}, true
	}
	return nil, false
}

// IfIntConverter performs int type enforcement: marks the conversion as
// successful if the value is already an int.
type IfIntConverter struct{}

var _ Converter = (*IfIntConverter)(nil)

// Convert returns int, true if the value is int.
// Returns nil, false otherwise.
func (*IfIntConverter) Convert(kv *types.KeyValue) (*types.KeyValue, bool) {
	if _, ok := kv.Value.(int); ok {
		return kv, true
	}
	return nil, false
}

// IfStrConverter performs string type enforcement: marks the conversion as
// successful if the value is already a string.
type IfStrConverter struct{}

var _ Converter = (*IfStrConverter)(nil)

// Convert returns string, true if the argument value is a string.
// Returns nil, false otherwise.
func (*IfStrConverter) Convert(kv *types.KeyValue) (*types.KeyValue, bool) {
	if _, ok := kv.Value.(string); ok {
		return kv, true
	}
	return nil, false
}

// IfBoolConverter performs bool type enforcement: marks the conversion as
// successfulk if the value is already a bool.
type IfBoolConverter struct{}

var _ Converter = (*IfBoolConverter)(nil)

// Convert returns bool, true if the value is a bool.
// Returns nil, false otherwise.
func (*IfBoolConverter) Convert(kv *types.KeyValue) (*types.KeyValue, bool) {
	if _, ok := kv.Value.(bool); ok {
		return kv, true
	}
	return nil, false
}

//======== Composite converters =======

// CompositionStrategy is a family of constants defining the logic of a
// composition chain.
type CompositionStrategy uint8

const (
	// CompNone means none of the chain components have to succeed. A no-op
	// logic, the chain always returns success.
	CompNone CompositionStrategy = iota
	// CompAnd means all components of the chain have to succeed in order to
	// mark the conversion successful. Returns the last successful conversion
	// result.
	CompAnd
	// CompOr means at least 1 component of the chain have to succeed in order
	// to mark the conversion successful. Returns the first successful conversion
	// result.
	CompOr
	// CompFirst means the conversion is marked successful if at least 1 component
	// returns success. Conversion chain terminates here.
	CompFirst
	// CompLast means the conversion tries to execute all chain components and
	// returns the last successful result.
	CompLast
)

// CompositeConverter implements a composite conversion logic defined by some
// specific conversion strategy.
type CompositeConverter struct {
	strategy   CompositionStrategy
	converters []Converter
}

// NewCompositeConverter is the constructor for a new CompositeStrategy chain.
// Accepts the conversion strategy and a list of conversion chain components.
func NewCompositeConverter(strategy CompositionStrategy, convs ...Converter) *CompositeConverter {
	return &CompositeConverter{
		strategy:   strategy,
		converters: convs,
	}
}

// Convert executes the conversion logic defined by the conversion strategy.
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
	// Identity is an initialized instance of IdentityConverter
	Identity *IdentityConverter

	// BoolPtrToBool is an initialized instance of BoolPtrToBoolConverter
	BoolPtrToBool *BoolPtrToBoolConverter
	// IntPtrToInt is an initialized instance of IntPtrToIntConverter
	IntPtrToInt *IntPtrToIntConverter
	// StrPtrToStr is an initialized instance of StrPtrToStrConverter
	StrPtrToStr *StrPtrToStrConverter

	// IntToBool is an initialized instance of IntToBoolConverter
	IntToBool *IntToBoolConverter
	// IntToStr is an initialized instance of IntToStrConverter
	IntToStr *IntToStrConverter
	// StrToBool is an initialized instance of StrToBoolConverter
	StrToBool *StrToBoolConverter
	// StrToInt is an initialized instance of StrToIntConveter
	StrToInt *StrToIntConverter

	// IfInt is an initialized instance of IfIntConverter
	IfInt *IfIntConverter
	// IfStr is an initialized instance of IfStrConverter
	IfStr *IfStrConverter
	// IfBool is an initialized instance of IfBoolConverter
	IfBool *IfBoolConverter

	// IntOrIntPtr is an instance of a composite converter enforcing an int or
	// an *int to int type.
	IntOrIntPtr *CompositeConverter
	// StrOrStrPtr is an instance of a composite converter enforcing a string
	// or a *string to string type.
	StrOrStrPtr *CompositeConverter
	// BoolOrBoolPtr is an instance of a composite converter enforcing a bool
	// or a *bool to bool type.
	BoolOrBoolPtr *CompositeConverter

	// ToInt is an instance of a composite converter enforcing an int, *int or
	// a string to int type.
	ToInt *CompositeConverter
	// ToStr is an instance of a composite converter enforcing a string, *string
	// or an int to string type.
	ToStr *CompositeConverter
	// ToBool is an instance of a composite converter enforcing a bool, *bool,
	// string or an int to bool value.
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
