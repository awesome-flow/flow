package explain

type Explainer interface {
	Explain(interface{}) ([]byte, error)
}
