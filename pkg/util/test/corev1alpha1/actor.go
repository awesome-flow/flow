package corev1alpha1

import (
	"sync"

	core "github.com/awesome-flow/flow/pkg/corev1alpha1"
)

type TestActorState string

const (
	TestActorStateNew     TestActorState = "new"
	TestActorStateStarted                = "started"
	TestActorStateStopped                = "stopped"
)

type TestActor struct {
	name      string
	peerscnt  int
	ctx       *core.Context
	lock      sync.Mutex
	params    core.Params
	state     TestActorState
	queue     chan *core.Message
	done      chan struct{}
	onstart   []func()
	onstop    []func()
	onconnect []func(int, core.Receiver)
	onreceive []func(*core.Message)
}

func NewTestActor(name string, ctx *core.Context, params core.Params) (core.Actor, error) {
	return &TestActor{
		name:      name,
		ctx:       ctx,
		params:    params,
		queue:     make(chan *core.Message, 1),
		done:      make(chan struct{}),
		state:     TestActorStateNew,
		onstart:   make([]func(), 0, 1),
		onstop:    make([]func(), 0, 1),
		onconnect: make([]func(int, core.Receiver), 0, 1),
		onreceive: make([]func(*core.Message), 0, 1),
	}, nil
}

func (t *TestActor) Name() string {
	return t.name
}

func (t *TestActor) Start() error {
	t.lock.Lock()
	defer t.lock.Unlock()
	t.state = TestActorStateStarted
	for _, h := range t.onstart {
		h()
	}

	return nil
}

func (t *TestActor) OnStart(h func()) {
	t.onstart = append(t.onstart, h)
}

func (t *TestActor) Stop() error {
	t.lock.Lock()
	defer t.lock.Unlock()
	close(t.queue)
	if t.peerscnt > 0 {
		<-t.done
	}
	t.state = TestActorStateStopped
	for _, h := range t.onstop {
		h()
	}

	return nil
}

func (t *TestActor) OnStop(h func()) {
	t.onstop = append(t.onstop, h)
}

func (t *TestActor) Connect(nthreads int, peer core.Receiver) error {
	t.lock.Lock()
	defer t.lock.Unlock()
	t.peerscnt++
	go func() {
		for msg := range t.queue {
			if err := peer.Receive(msg); err != nil {
				t.ctx.Logger().Error(err.Error())
			}
		}
		close(t.done)
	}()
	for _, h := range t.onconnect {
		h(nthreads, peer)
	}

	return nil
}

func (t *TestActor) OnConnect(h func(int, core.Receiver)) {
	t.onconnect = append(t.onconnect, h)
}

func (t *TestActor) Receive(msg *core.Message) error {
	t.queue <- msg
	for _, h := range t.onreceive {
		h(msg)
	}

	return nil
}

func (t *TestActor) OnReceive(h func(*core.Message)) {
	t.onreceive = append(t.onreceive, h)
}

func (t *TestActor) State() TestActorState {
	return t.state
}

func (t *TestActor) Flush() {
	for len(t.queue) > 0 {
		<-t.queue
	}
}
