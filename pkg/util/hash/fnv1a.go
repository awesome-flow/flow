package hash

// <3 Fabian https://github.com/grobian/carbon-c-relay/blob/master/fnv1a.h

const (
	FNV1AOffset32 = 216613626
	FNV1APrime32  = 16777619

	FNV1AOffset64 = 14695981039346656037
	FNV1APrime64  = 1099511628211
)

func Fnv1a32(key string) uint32 {
	hash := uint32(FNV1AOffset32)
	for i := 0; i < len(key); i++ {
		hash = (hash ^ uint32(key[i])) * FNV1APrime32
	}
	return hash
}

func Fnv1a64(key string) uint64 {
	hash := uint64(FNV1AOffset64)
	for i := 0; i < len(key); i++ {
		hash = (hash ^ uint64(key[i])) * FNV1APrime64
	}
	return hash
}
