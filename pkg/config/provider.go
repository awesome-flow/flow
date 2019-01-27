package config

type ProviderOptions uint64

const (
	ProviderOptionsFileCache ProviderOptions = 1 << iota
	ProviderOptionsTrustOldCache

	ProviderOptionsNone = 0
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

type ProviderList []Provider

func (p ProviderList) Len() int {
	return len(p)
}

func (p ProviderList) Swap(i, j int) {
	p[i], p[j] = p[j], p[i]
}

func (p ProviderList) Less(i, j int) bool {
	return p[i].GetWeight() < p[j].GetWeight()
}
