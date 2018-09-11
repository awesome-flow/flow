package hash

// <3 Damian
// https://medium.com/@dgryski/consistent-hashing-algorithmic-tradeoffs-ef6b8e2fcae8

func JumpHash(key uint64, numBuckets int) uint {
	var (
		b int64 = -1
		j int64
	)
	for j < int64(numBuckets) {
		b = j
		key = key*28629335557779417 + 1
		j = int64(float64(b+1) * (float64(1<<31) / float64((key>>33)+1)))
	}
	return uint(b)
}
