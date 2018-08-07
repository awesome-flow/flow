package config

type ProviderOptions uint64

const (
	ProviderOptionsFileCache ProviderOptions = 1 << iota
	ProviderOptionsTrustOldCache
)

type Provider interface {
	Setup() error
	DependsOn() []string
	Resolve() error
	GetName() string
	GetWeight() uint32
	GetOptions() ProviderOptions
	GetValue(string) (interface{}, bool)
}
