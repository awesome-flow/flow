package actor

import (
	"fmt"

	core "github.com/awesome-flow/flow/pkg/corev1alpha1"
)

type SinkNull struct {
	name string
	ctx  *core.Context
}

var _ core.Actor = (*SinkNull)(nil)

func NewSinkNull(name string, ctx *core.Context, _ core.Params) (core.Actor, error) {
	return &SinkNull{
		name: name,
		ctx:  ctx,
	}, nil
}

func (n *SinkNull) Name() string {
	return n.name
}

func (n *SinkNull) Start() error {
	return nil
}

func (n *SinkNull) Stop() error {
	return nil
}

func (n *SinkNull) Connect(int, core.Receiver) error {
	return fmt.Errorf("sink %q can not connect to other receivers", n.name)
}

func (n *SinkNull) Receive(msg *core.Message) error {
	msg.Complete(core.MsgStatusDone)
	return nil
}
