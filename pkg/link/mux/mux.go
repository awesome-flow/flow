package link

import (
	"github.com/awesome-flow/flow/pkg/core"
)

type Mux struct {
	Name string
	*core.Connector
}

func New(name string, _ core.Params, context *core.Context) (core.Link, error) {
	mux := &Mux{
		name,
		core.NewConnectorWithContext(context),
	}
	return mux, nil
}

func (mux *Mux) LinkTo(links []core.Link) error {
	for _, link := range links {
		if err := link.ConnectTo(mux); err != nil {
			return err
		}
	}
	return nil
}
