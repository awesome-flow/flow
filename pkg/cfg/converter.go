package cfg

import "strconv"

type Converter interface {
	Convert(kv *KeyValue) (*KeyValue, bool)
}

type IdentityConverter struct{}

var _ Converter = (*IdentityConverter)(nil)

func (id *IdentityConverter) Convert(kv *KeyValue) (*KeyValue, bool) {
	return kv, true
}

type PtrToIntConverter struct{}

var _ Converter = (*PtrToIntConverter)(nil)

func (pi *PtrToIntConverter) Convert(kv *KeyValue) (*KeyValue, bool) {
	if pv, ok := kv.Value.(*int); ok {
		return &KeyValue{kv.Key, *pv}, true
	}
	return nil, false
}

type StrToIntConverter struct{}

var _ Converter = (*StrToIntConverter)(nil)

func (si *StrToIntConverter) Convert(kv *KeyValue) (*KeyValue, bool) {
	if sv, ok := kv.Value.(string); ok {
		s, err := strconv.Atoi(sv)
		if err == nil {
			return &KeyValue{kv.Key, s}, true
		}
	}
	return nil, false
}

type StrPtrToStrConverter struct{}

var _ Converter = (*StrPtrToStrConverter)(nil)

func (sps *StrPtrToStrConverter) Convert(kv *KeyValue) (*KeyValue, bool) {
	if spv, ok := kv.Value.(*string); ok {
		return &KeyValue{kv.Key, *spv}, true
	}
	return nil, false
}

type IntToStrConverter struct{}

var _ Converter = (*IntToStrConverter)(nil)

func (is *IntToStrConverter) Convert(kv *KeyValue) (*KeyValue, bool) {
	if iv, ok := kv.Value.(int); ok {
		return &KeyValue{kv.Key, strconv.Itoa(iv)}, true
	}
	return nil, false
}
