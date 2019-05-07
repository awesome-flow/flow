package sink

import (
	"github.com/awesome-flow/flow/pkg/core"
	"github.com/awesome-flow/flow/pkg/types"
)

type Null struct {
	Name string
	*core.Connector
}

func New(name string, _ types.Params, conext *core.Context) (core.Link, error) {
	return &Null{name, core.NewConnectorWithContext(conext)}, nil
}

func (n *Null) Recv(msg *core.Message) error {
	return msg.AckDone()
}

func (n *Null) ConnectTo(core.Link) error {
	panic("/dev/null is not supposed to be connected to other links")
}
