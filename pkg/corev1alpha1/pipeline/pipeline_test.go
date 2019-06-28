package pipeline

import (
	"sync"
	"testing"

	"github.com/awesome-flow/flow/pkg/cfg"
	core "github.com/awesome-flow/flow/pkg/corev1alpha1"
	"github.com/awesome-flow/flow/pkg/util/data"
)

type TestActor struct {
	name   string
	ctx    *core.Context
	lock   sync.Mutex
	params core.Params
	state  string
	queue  chan *core.Message
	done   chan struct{}
}

func NewTestActor(name string, ctx *core.Context, params core.Params) (core.Actor, error) {
	return &TestActor{
		name:   name,
		ctx:    ctx,
		params: params,
		queue:  make(chan *core.Message),
		done:   make(chan struct{}),
		state:  "initialized",
	}, nil
}

func (t *TestActor) Name() string {
	return t.name
}

func (t *TestActor) Start() error {
	t.lock.Lock()
	defer t.lock.Unlock()
	t.state = "started"

	return nil
}

func (t *TestActor) Stop() error {
	t.lock.Lock()
	defer t.lock.Unlock()
	close(t.queue)
	<-t.done
	t.state = "stopped"

	return nil
}

func (t *TestActor) Connect(nthreads int, peer core.Receiver) error {
	t.lock.Lock()
	defer t.lock.Unlock()
	go func() {
		for msg := range t.queue {
			if err := peer.Receive(msg); err != nil {
				t.ctx.Logger().Error(err.Error())
			}
		}
		close(t.done)
	}()

	return nil
}

func (t *TestActor) Receive(msg *core.Message) error {
	t.queue <- msg

	return nil
}

func TestStartPropogatesToActors(t *testing.T) {
	//builders = map[string]core.Builder{
	//	"core.test.actor": NewTestActor,
	//}
	repo := cfg.NewRepository()
	cfg := core.NewConfig(repo)
	ctx, _ := core.NewContext(cfg)
	if err := ctx.Start(); err != nil {
		t.Fatalf("failed to start context: %s", err)
	}
	act, err := NewTestActor("test-actor", ctx, nil)
	if err != nil {
		t.Fatalf("failed to create a test actor: %s", err)
	}
	top := data.NewTopology()
	top.AddNode(act)
	p := &Pipeline{
		ctx: ctx,
		actors: map[string]core.Actor{
			"test-actor": act,
		},
		topology: top,
	}
	if err := p.Start(); err != nil {
		t.Fatalf("failed to start pipeline: %s", err)
	}
	if st := act.(*TestActor).state; st != "started" {
		t.Fatalf("actor is in a wrong state: got %s. want: %s", st, "started")
	}
}
