package pipeline

import (
	"reflect"
	"sync"
	"testing"

	"github.com/awesome-flow/flow/pkg/cfg"
	core "github.com/awesome-flow/flow/pkg/corev1alpha1"
	"github.com/awesome-flow/flow/pkg/util/data"
)

type TestActor struct {
	name      string
	ctx       *core.Context
	lock      sync.Mutex
	params    core.Params
	state     string
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
		queue:     make(chan *core.Message),
		done:      make(chan struct{}),
		state:     "initialized",
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
	t.state = "started"
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
	<-t.done
	t.state = "stopped"
	for _, h := range t.onstop {
		h()
	}
	t.ctx.Logger().Info("test actor %s has been stopped", t.name)

	return nil
}

func (t *TestActor) OnStop(h func()) {
	t.onstop = append(t.onstop, h)
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

func TestStartPropogatesToActors(t *testing.T) {
	repo := cfg.NewRepository()
	cfg := core.NewConfig(repo)
	ctx, _ := core.NewContext(cfg)
	if err := ctx.Start(); err != nil {
		t.Fatalf("failed to start context: %s", err)
	}

	act1, err := NewTestActor("test-actor-1", ctx, nil)
	if err != nil {
		t.Fatalf("failed to create a test actor: %s", err)
	}
	act2, err := NewTestActor("test-actor-2", ctx, nil)
	if err != nil {
		t.Fatalf("failed to create a test actor: %s", err)
	}

	act1.Connect(1, act2)

	top := data.NewTopology()
	top.AddNode(act1)
	top.AddNode(act2)
	top.Connect(act1, act2)

	events := make([]string, 0, 2)

	act1.(*TestActor).OnStart(func() {
		events = append(events, act1.Name())
	})

	act2.(*TestActor).OnStart(func() {
		events = append(events, act2.Name())
	})

	p := &Pipeline{
		ctx: ctx,
		actors: map[string]core.Actor{
			"test-actor-1": act1,
			"test-actor-2": act1,
		},
		topology: top,
	}

	if err := p.Start(); err != nil {
		t.Fatalf("failed to start the pipeline: %s", err)
	}

	wantevents := []string{"test-actor-2", "test-actor-1"}

	if !reflect.DeepEqual(events, wantevents) {
		t.Fatalf("unexpected events: got: %v, want: %v", events, wantevents)
	}
}

func TestStopPropogatesToActors(t *testing.T) {
	repo := cfg.NewRepository()
	cfg := core.NewConfig(repo)
	ctx, _ := core.NewContext(cfg)
	if err := ctx.Start(); err != nil {
		t.Fatalf("failed to start context: %s", err)
	}

	act1, err := NewTestActor("test-actor-1", ctx, nil)
	if err != nil {
		t.Fatalf("failed to create a test actor: %s", err)
	}
	act2, err := NewTestActor("test-actor-2", ctx, nil)
	if err != nil {
		t.Fatalf("failed to create a test actor: %s", err)
	}

	act1.Connect(1, act2)

	top := data.NewTopology()
	top.AddNode(act1)
	top.AddNode(act2)
	top.Connect(act1, act2)

	events := make([]string, 0, 2)

	act1.(*TestActor).OnStop(func() {
		events = append(events, act1.Name())
	})

	act2.(*TestActor).OnStop(func() {
		events = append(events, act2.Name())
	})

	p := &Pipeline{
		ctx: ctx,
		actors: map[string]core.Actor{
			"test-actor-1": act1,
			"test-actor-2": act1,
		},
		topology: top,
	}

	if err := p.Start(); err != nil {
		t.Fatalf("failed to start the pipeline: %s", err)
	}

	if err := p.Stop(); err != nil {
		t.Fatalf("failed to stop the pipeline: %s", err)
	}

	wantevents := []string{"test-actor-1", "test-actor-2"}

	if !reflect.DeepEqual(events, wantevents) {
		t.Fatalf("unexpected events: got: %v, want: %v", events, wantevents)
	}
}
