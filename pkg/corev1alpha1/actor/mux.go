package actor

import core "github.com/awesome-flow/flow/pkg/corev1alpha1"

type Mux struct {
	name  string
	ctx   *core.Context
	queue chan *core.Message
}

var _ core.Actor = (*Mux)(nil)

func NewMux(name string, ctx *core.Context, params core.Params) (core.Actor, error) {
	return &Mux{
		name:  name,
		ctx:   ctx,
		queue: make(chan *core.Message),
	}, nil
}

func (m *Mux) Name() string {
	return m.name
}

func (m *Mux) Start() error {
	return nil
}

func (m *Mux) Stop() error {
	close(m.queue)
	return nil
}

func (m *Mux) Connect(nthreads int, peer core.Receiver) error {
	for i := 0; i < nthreads; i++ {
		go func() {
			for msg := range m.queue {
				if err := peer.Receive(msg); err != nil {
					m.ctx.Logger().Error(err.Error())
				}
			}
		}()
	}
	return nil
}

func (m *Mux) Receive(msg *core.Message) error {
	m.queue <- msg
	return nil
}
