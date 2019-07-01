package pipeline

import (
	"reflect"
	"testing"

	"github.com/awesome-flow/flow/pkg/cfg"
	core "github.com/awesome-flow/flow/pkg/corev1alpha1"
	"github.com/awesome-flow/flow/pkg/types"
	"github.com/awesome-flow/flow/pkg/util/data"
	flowplugin "github.com/awesome-flow/flow/pkg/util/plugin"
	flowtest "github.com/awesome-flow/flow/pkg/util/test/corev1alpha1"
)

func TestStartPropogatesToActors(t *testing.T) {
	repo := cfg.NewRepository()
	cfg := core.NewConfig(repo)
	ctx, _ := core.NewContext(cfg)
	if err := ctx.Start(); err != nil {
		t.Fatalf("failed to start context: %s", err)
	}
	defer ctx.Stop()

	act1, err := flowtest.NewTestActor("test-actor-1", ctx, nil)
	if err != nil {
		t.Fatalf("failed to create a test actor: %s", err)
	}
	act2, err := flowtest.NewTestActor("test-actor-2", ctx, nil)
	if err != nil {
		t.Fatalf("failed to create a test actor: %s", err)
	}

	act1.Connect(1, act2)

	top := data.NewTopology()
	top.AddNode(act1)
	top.AddNode(act2)
	top.Connect(act1, act2)

	events := make([]string, 0, 2)

	act1.(*flowtest.TestActor).OnStart(func() {
		events = append(events, act1.Name())
	})

	act2.(*flowtest.TestActor).OnStart(func() {
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
	defer ctx.Stop()

	act1, err := flowtest.NewTestActor("test-actor-1", ctx, nil)
	if err != nil {
		t.Fatalf("failed to create a test actor: %s", err)
	}
	act2, err := flowtest.NewTestActor("test-actor-2", ctx, nil)
	if err != nil {
		t.Fatalf("failed to create a test actor: %s", err)
	}

	act1.Connect(1, act2)

	top := data.NewTopology()
	top.AddNode(act1)
	top.AddNode(act2)
	top.Connect(act1, act2)

	events := make([]string, 0, 2)

	act1.(*flowtest.TestActor).OnStop(func() {
		events = append(events, act1.Name())
	})

	act2.(*flowtest.TestActor).OnStop(func() {
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

func TestBuildActors(t *testing.T) {
	coreActorName := "test-core-actor-1"
	pluginActorName := "test-plugin-actor-1"

	factories := map[string]ActorFactory{
		"core": NewCoreActorFactoryWithBuilders(
			map[string]core.Builder{
				"core.test-actor": flowtest.NewTestActor,
			},
		),
		"plugin": NewPluginActorFactoryWithLoader(
			func(path, name string) (flowplugin.Plugin, error) {
				return &flowtest.TestPlugin{
					Path: path,
					Name: name,
				}, nil
			},
		),
	}
	repo := cfg.NewRepository()
	ctx, err := core.NewContext(core.NewConfig(repo))
	if err != nil {
		t.Fatalf("failed to create a context: %s", err)
	}
	actorscfg := map[string]types.CfgBlockActor{
		coreActorName: types.CfgBlockActor{
			Module: "core.test-actor",
		},
		pluginActorName: types.CfgBlockActor{
			Module: "plugin.test-plugin",
		},
	}

	if _, err := cfg.NewScalarConfigProvider(
		&types.KeyValue{
			Key:   types.NewKey("actors"),
			Value: actorscfg,
		},
		repo,
		42, // doesn't matter
	); err != nil {
		t.Fatalf("failed to create scalar provider: %s", err)
	}
	if _, err := cfg.NewScalarConfigProvider(
		&types.KeyValue{
			Key:   types.NewKey("plugin.path"),
			Value: "/never/where",
		},
		repo,
		42, // doesn't matter
	); err != nil {
		t.Fatalf("failed to create scalar provider: %s", err)
	}

	if err := ctx.Start(); err != nil {
		t.Fatalf("failed to start context: %s", err)
	}
	defer ctx.Stop()

	actors, err := buildActors(ctx, factories)
	if err != nil {
		t.Fatalf("failed to build actors: %s", err)
	}

	if len(actors) > 2 {
		t.Fatalf("Unexpected contents of actors map: %+v", actors)
	}

	for _, name := range []string{coreActorName, pluginActorName} {
		if _, ok := actors[name]; !ok {
			t.Fatalf("actor %s is missing from actors map", name)
		}
		if _, ok := actors[name].(*flowtest.TestActor); !ok {
			t.Fatalf("unexpected actor type: got: %s, want: %s", reflect.TypeOf(actors[name]), "*pipeline.TestActor")
		}
		if actorname := actors[name].Name(); actorname != name {
			t.Fatalf("unexpected actor name: got: %s, want: %s", actors[name].Name(), name)
		}
	}
}
