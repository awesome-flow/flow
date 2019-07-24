package pipeline

import (
	"fmt"
	"strings"

	core "github.com/awesome-flow/flow/pkg/corev1alpha1"
	"github.com/awesome-flow/flow/pkg/corev1alpha1/actor"
	"github.com/awesome-flow/flow/pkg/types"
	flowplugin "github.com/awesome-flow/flow/pkg/util/plugin"
)

var CoreBuilders map[string]core.Builder = map[string]core.Builder{
	"core.receiver.http": actor.NewReceiverHTTP,
	"core.receiver.tcp":  actor.NewReceiverTCP,
	"core.receiver.udp":  actor.NewReceiverUDP,
	"core.receiver.unix": actor.NewReceiverUnix,

	"core.buffer":     actor.NewBuffer,
	"core.compressor": actor.NewCompressor,
	"core.mux":        actor.NewMux,
	"core.replicator": actor.NewReplicator,
	"core.router":     actor.NewRouter,
	"core.throttler":  actor.NewThrottler,

	"core.sink.dumper": actor.NewSinkDumper,
	"core.sink":        actor.NewSink,
}

type ActorFactory interface {
	Build(name string, ctx *core.Context, cfg *types.CfgBlockActor) (core.Actor, error)
}

type CoreActorFactory struct {
	builders map[string]core.Builder
}

var _ ActorFactory = (*CoreActorFactory)(nil)

func NewCoreActorFactory() *CoreActorFactory {
	return NewCoreActorFactoryWithBuilders(CoreBuilders)
}

func NewCoreActorFactoryWithBuilders(builders map[string]core.Builder) *CoreActorFactory {
	return &CoreActorFactory{
		builders: builders,
	}
}

func (f *CoreActorFactory) Build(name string, ctx *core.Context, cfg *types.CfgBlockActor) (core.Actor, error) {
	module := cfg.Module
	if _, ok := f.builders[module]; !ok {
		return nil, fmt.Errorf("unrecognised core module %[2]s for actor %[1]s", name, module)
	}

	return (f.builders[module])(name, ctx, core.Params(cfg.Params))
}

type PluginActorFactory struct {
	loader flowplugin.Loader
}

var _ ActorFactory = (*PluginActorFactory)(nil)

func NewPluginActorFactory() *PluginActorFactory {
	return NewPluginActorFactoryWithLoader(flowplugin.GoPluginLoader)
}

func NewPluginActorFactoryWithLoader(loader flowplugin.Loader) *PluginActorFactory {
	return &PluginActorFactory{
		loader: loader,
	}
}

func (f *PluginActorFactory) Build(name string, ctx *core.Context, cfg *types.CfgBlockActor) (core.Actor, error) {
	module := cfg.Module
	module = strings.Replace(module, "plugin.", "", 1)
	path, ok := ctx.Config().Get(types.NewKey("plugin.path"))
	if !ok {
		return nil, fmt.Errorf("failed to fetch config for `plugin.path`")
	}
	plugin, err := f.loader(path.(string), module)
	if err != nil {
		return nil, err
	}
	builder, err := plugin.Lookup(cfg.Builder)
	if err != nil {
		return nil, fmt.Errorf("lookup for method `%s` failed for plugin %s", cfg.Builder, cfg.Module)
	}
	return builder.(func(string, *core.Context, core.Params) (core.Actor, error))(name, ctx, core.Params(cfg.Params))
}
