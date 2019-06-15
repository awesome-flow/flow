package actor

import (
	"sync"

	core "github.com/awesome-flow/flow/pkg/corev1alpha1"
)

type Fanout struct {
	name   string
	ctx    *core.Context
	nq     int
	iq     int
	lock   sync.RWMutex
	queues []chan *core.Message
}

var _ core.Actor = (*Fanout)(nil)

func NewFanout(name string, ctx *core.Context, params core.Params) (core.Actor, error) {
	return &Fanout{
		name:   name,
		ctx:    ctx,
		queues: make([]chan *core.Message, 0, 1),
	}, nil
}

func (f *Fanout) Name() string {
	return f.name
}

func (f *Fanout) Start() error {
	return nil
}

func (f *Fanout) Stop() error {
	for _, queue := range f.queues {
		close(queue)
	}
	return nil
}

func (f *Fanout) Connect(nthreads int, peer core.Receiver) error {
	f.lock.Lock()
	defer f.lock.Unlock()
	queue := make(chan *core.Message)
	for i := 0; i < nthreads; i++ {
		go func() {
			for msg := range queue {
				if err := peer.Receive(msg); err != nil {
					f.ctx.Logger().Error(err.Error())
				}
			}
		}()
	}
	f.queues = append(f.queues, queue)
	f.nq++

	return nil
}

func (f *Fanout) Receive(msg *core.Message) error {
	f.lock.RLock()
	ix := f.iq
	f.iq = (f.iq + 1) % f.nq
	f.lock.RUnlock()
	f.queues[ix] <- msg

	return nil
}
