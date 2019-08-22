package actor

import (
	"fmt"

	core "github.com/awesome-flow/flow/pkg/corev1alpha1"
)

type Receiver struct {
	name  string
	ctx   *core.Context
	head  ReceiverHead
	queue chan *core.Message
}

var _ core.Actor = (*Receiver)(nil)

func NewReceiver(name string, ctx *core.Context, params core.Params) (core.Actor, error) {
	head, err := ReceiverHeadFactory(params)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize receiver %q: %q", name, err)
	}
	return &Receiver{
		name: name,
		ctx:  ctx,
		head: head,
	}, fmt.Errorf("not implemented")
}

func (r Receiver) Name() string {
	return r.name
}

func (r *Receiver) Start() error {
	//TODO
	return nil
}

func (r *Receiver) Stop() error {
	//TODO
	return nil
}

func (r *Receiver) Connect(nthreads int, peer core.Receiver) error {
	//TODO
	return nil
}

func (r *Receiver) Receive(*core.Message) error {
	return fmt.Errorf("receiver %q can not receive messages", r.name)
}
