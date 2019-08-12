package actor

import (
	"sync"

	core "github.com/awesome-flow/flow/pkg/corev1alpha1"
)

type Mux struct {
	name  string
	ctx   *core.Context
	queue chan *core.Message
	wg    sync.WaitGroup
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
	m.wg.Wait()
	return nil
}

func (m *Mux) Connect(nthreads int, peer core.Receiver) error {
	for i := 0; i < nthreads; i++ {
		m.wg.Add(1)
		go func() {
			for msg := range m.queue {
				if err := peer.Receive(msg); err != nil {
					m.ctx.Logger().Error(err.Error())
				}
			}
			m.wg.Done()
		}()
	}
	return nil
}

func (m *Mux) Receive(msg *core.Message) error {
	m.queue <- msg
	return nil
}
