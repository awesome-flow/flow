package pipeline

import (
	"fmt"
	"reflect"
	"sync"
	"testing"

	"github.com/awesome-flow/flow/pkg/cfg"
	core "github.com/awesome-flow/flow/pkg/corev1alpha1"
	"github.com/awesome-flow/flow/pkg/types"
	"github.com/awesome-flow/flow/pkg/util/data"
	flowplugin "github.com/awesome-flow/flow/pkg/util/plugin"
)

type TestActor struct {
	name      string
	peerscnt  int
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
	if t.peerscnt > 0 {
		<-t.done
	}
	t.state = "stopped"
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

func TestBuildCoreActor(t *testing.T) {
	builders := map[string]core.Builder{
		"test-actor": NewTestActor,
	}
	repo := cfg.NewRepository()
	ctx, err := core.NewContext(core.NewConfig(repo))
	if err != nil {
		t.Fatalf("failed to create a context: %s", err)
	}
	actorcfg := &types.CfgBlockActor{
		Module: "test-actor",
	}
	actorname := "test-actor-1"
	a, err := buildCoreActor(builders, actorname, ctx, actorcfg)
	if err != nil {
		t.Fatalf("failed to build core actor: %s", err)
	}
	if _, ok := a.(core.Actor); !ok {
		t.Fatalf("actor does not conform to core.Actor interface: %s", reflect.TypeOf(a))
	}
	if _, ok := a.(*TestActor); !ok {
		t.Fatalf("unexpected actor type: got: %s, want: %s", reflect.TypeOf(a), reflect.TypeOf(new(TestActor)))
	}
	if name := a.Name(); name != actorname {
		t.Fatalf("unexpected actor name: got: %s, want: %s", name, actorname)
	}
}

type TestPlugin struct {
	path string
}

func (p *TestPlugin) Load() error {
	return nil
}

func (p *TestPlugin) Lookup(symName string) (flowplugin.Symbol, error) {
	return NewTestActor, nil
}

func MockPluginLoader(path string) (flowplugin.Plugin, error) {
	return &TestPlugin{path: path}, nil
}

type ScalarConfigProvider struct {
	kv *types.KeyValue
}

var _ cfg.Provider = (*ScalarConfigProvider)(nil)

func NewScalarConfigProvider(kv *types.KeyValue) *ScalarConfigProvider {
	return &ScalarConfigProvider{
		kv: kv,
	}
}

func (s *ScalarConfigProvider) Name() string {
	return fmt.Sprintf("scalar-provider-%s", s.kv.Key)
}

func (s *ScalarConfigProvider) Depends() []string {
	return []string{}
}

func (*ScalarConfigProvider) SetUp(repo *cfg.Repository) error {
	return nil
}

func (*ScalarConfigProvider) TearDown(*cfg.Repository) error {
	return nil
}

func (s *ScalarConfigProvider) Get(key types.Key) (*types.KeyValue, bool) {
	if key.Equals(s.kv.Key) {
		return s.kv, true
	}

	return nil, false
}

func (s *ScalarConfigProvider) Weight() int {
	return 42
}

func TestBuildPluginActor(t *testing.T) {
	repo := cfg.NewRepository()
	ctx, err := core.NewContext(core.NewConfig(repo))
	if err != nil {
		t.Fatalf("failed to create a context: %s", err)
	}
	if err := ctx.Start(); err != nil {
		t.Fatalf("failed to start context: %s", err)
	}
	key := types.NewKey("plugin.path")
	kv := &types.KeyValue{Key: key, Value: "/no/where"}
	prov := NewScalarConfigProvider(kv)
	if err := repo.RegisterKey(key, prov); err != nil {
		t.Fatalf("failed to register a key in repo: %s", err)
	}
	plugincfg := &types.CfgBlockActor{
		Module: "plugin.test-plugin",
	}
	actorname := "test-plugin-1"
	a, err := buildPluginActor(MockPluginLoader, actorname, ctx, plugincfg)
	if err != nil {
		t.Fatalf("failed to build plugin actor: %s", err)
	}
	if _, ok := a.(*TestActor); !ok {
		t.Fatalf("unexpected actor type: got: %s, want: %s", reflect.TypeOf(a), reflect.TypeOf(new(TestActor)))
	}
	if name := a.Name(); name != actorname {
		t.Fatalf("unexpected actor name: got: %s, want: %s", name, actorname)
	}
}
