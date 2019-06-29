package pipeline

import (
	"reflect"
	"testing"

	"github.com/awesome-flow/flow/pkg/cfg"
	core "github.com/awesome-flow/flow/pkg/corev1alpha1"
	"github.com/awesome-flow/flow/pkg/types"
	flowplugin "github.com/awesome-flow/flow/pkg/util/plugin"
)

func TestCoreActorFctoryBuild(t *testing.T) {
	module := "test.builder"
	builders := map[string]core.Builder{
		module: func(name string, ctx *core.Context, params core.Params) (core.Actor, error) {
			return NewTestActor(name, ctx, params)
		},
	}
	factory := NewCoreActorFactoryWithBuilders(builders)
	name := "test-actor-1"
	repo := cfg.NewRepository()
	ctx, err := core.NewContext(core.NewConfig(repo))
	if err != nil {
		t.Fatalf("failed to create a context: %s", err)
	}
	if err := ctx.Start(); err != nil {
		t.Fatalf("failed to start context: %s", err)
	}
	defer ctx.Stop()
	actcfg := &types.CfgBlockActor{
		Module: module,
	}
	actor, err := factory.Build(name, ctx, actcfg)
	if err != nil {
		t.Fatalf("failed to build actor: %s", err)
	}
	if _, ok := actor.(*TestActor); !ok {
		t.Fatalf("unexpected type of actor: got: %s, want: %s", reflect.TypeOf(actor).String(), "TestActor")
	}
	if actor.Name() != name {
		t.Fatalf("unexpected actor name: got: %s, want: %s", actor.Name(), name)
	}
}

func TestPluginActorFactoryBuild(t *testing.T) {
	name := "test-plugin-actor-1"
	loader := func(path, name string) (flowplugin.Plugin, error) {
		return &TestPlugin{
			path: path,
			name: name,
		}, nil
	}
	repo := cfg.NewRepository()
	if _, err := cfg.NewScalarConfigProvider(
		&types.KeyValue{
			Key:   types.NewKey("plugin.path"),
			Value: "/never/where",
		},
		repo,
		42, // Doesn't matter: it's the only provider
	); err != nil {
		t.Fatalf("failed to create scalar config: %s", err)
	}
	factory := NewPluginActorFactoryWithLoader(loader)
	ctx, err := core.NewContext(core.NewConfig(repo))
	if err != nil {
		t.Fatalf("failed to create a context: %s", err)
	}
	if err := ctx.Start(); err != nil {
		t.Fatalf("failed to start context: %s", err)
	}
	defer ctx.Stop()
	actcfg := &types.CfgBlockActor{
		Module: "plugin.test-actor",
	}
	actor, err := factory.Build(name, ctx, actcfg)
	if err != nil {
		t.Fatalf("failed to build actor: %s", err)
	}
	if _, ok := actor.(*TestActor); !ok {
		t.Fatalf("unexpected type of actor: got: %s, want: %s", reflect.TypeOf(actor).String(), "*pipeline.TestActor")
	}
	if actor.Name() != name {
		t.Fatalf("unexpected actor name: got: %s, want: %s", actor.Name(), name)
	}
}
