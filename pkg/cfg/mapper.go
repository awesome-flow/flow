package cfg

type Mapper interface {
	Map(kv *KeyValue) (*KeyValue, error)
}
