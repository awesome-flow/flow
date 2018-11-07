package sink

import "github.com/whiteboxio/flow/pkg/core"

type Null struct {
	Name string
	*core.Connector
}

func New(name string, _ core.Params, conext *core.Context) (core.Link, error) {
	return &Null{name, core.NewConnectorWithContext(conext)}, nil
}

func (n *Null) Recv(msg *core.Message) error {
	return msg.AckDone()
}

func (n *Null) ConnectTo(core.Link) error {
	panic("/dev/null is not supposed to be connected to other links")
}
