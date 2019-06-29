package pipeline

import (
	"reflect"
	"testing"

	"github.com/awesome-flow/flow/pkg/cfg"
	core "github.com/awesome-flow/flow/pkg/corev1alpha1"
	"github.com/awesome-flow/flow/pkg/util/data"
)

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
