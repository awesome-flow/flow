package actor

import (
	"sync"

	core "github.com/awesome-flow/flow/pkg/corev1alpha1"
)

type Router struct {
	name  string
	ctx   *core.Context
	rtmap map[string]chan *core.Message
	lock  sync.Mutex
	wg    sync.WaitGroup
}

var _ core.Actor = (*Router)(nil)

func NewRouter(name string, ctx *core.Context, params core.Params) (core.Actor, error) {
	return &Router{
		name:  name,
		ctx:   ctx,
		rtmap: make(map[string]chan *core.Message),
		lock:  sync.Mutex{},
	}, nil
}

func (r *Router) Name() string {
	return r.name
}

func (r *Router) Start() error {
	return nil
}

func (r *Router) Stop() error {
	for _, ch := range r.rtmap {
		close(ch)
	}
	r.wg.Wait()
	return nil
}

func (r *Router) Connect(nthreads int, peer core.Receiver) error {
	r.lock.Lock()
	defer r.lock.Unlock()
	peername := peer.(core.Namer).Name()
	if _, ok := r.rtmap[peername]; !ok {
		r.rtmap[peername] = make(chan *core.Message)
	}
	queue := r.rtmap[peername]
	for i := 0; i < nthreads; i++ {
		r.wg.Add(1)
		go func() {
			for msg := range queue {
				if err := peer.Receive(msg); err != nil {
					msg.Complete(core.MsgStatusFailed)
					r.ctx.Logger().Error(err.Error())
				}
			}
			r.wg.Done()
		}()
	}
	return nil
}

func (r *Router) Receive(msg *core.Message) error {
	if rtkey, ok := msg.Meta("sendto"); ok {
		if queue, ok := r.rtmap[rtkey.(string)]; ok {
			queue <- msg
			return nil
		}
	}
	msg.Complete(core.MsgStatusUnroutable)
	return nil
}
