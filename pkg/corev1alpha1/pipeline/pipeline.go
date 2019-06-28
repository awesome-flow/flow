package pipeline

import (
	"fmt"
	"path"
	"strings"

	core "github.com/awesome-flow/flow/pkg/corev1alpha1"
	"github.com/awesome-flow/flow/pkg/corev1alpha1/actor"
	"github.com/awesome-flow/flow/pkg/types"
	"github.com/awesome-flow/flow/pkg/util/data"
	flowplugin "github.com/awesome-flow/flow/pkg/util/plugin"
)

var CoreBuilders map[string]core.Builder = map[string]core.Builder{
	"core.receiver.tcp":  actor.NewReceiverTCP,
	"core.receiver.udp":  actor.NewReceiverUDP,
	"core.receiver.http": actor.NewReceiverHTTP,
	"core.receiver.unix": actor.NewReceiverUnix,

	"core.demux":      actor.NewDemux,
	"core.mux":        actor.NewMux,
	"core.router":     actor.NewRouter,
	"core.throttler":  actor.NewThrottler,
	"core.fanout":     actor.NewFanout,
	"core.buffer":     actor.NewBuffer,
	"core.compressor": actor.NewCompressor,

	"core.sink.dumper": actor.NewSinkDumper,
	"core.sink.tcp":    actor.NewSinkTCP,
	"core.sink.udp":    actor.NewSinkUDP,
	"core.sink.null":   actor.NewSinkNull,
}

type Pipeline struct {
	ctx          *core.Context
	actors       map[string]core.Actor
	topology     *data.Topology
	pluginloader func(string) (flowplugin.Plugin, error)
}

var _ core.Runner = (*Pipeline)(nil)

func NewPipeline(ctx *core.Context) (*Pipeline, error) {
	return NewPipelineWithBuilders(ctx, CoreBuilders)
}

func NewPipelineWithBuilders(ctx *core.Context, builders map[string]core.Builder) (*Pipeline, error) {
	actors, err := buildActors(ctx, builders, flowplugin.GoPluginLoader)
	if err != nil {
		return nil, err
	}

	topology, err := buildTopology(ctx, actors)
	if err != nil {
		return nil, err
	}

	p := &Pipeline{
		ctx:          ctx,
		actors:       actors,
		topology:     topology,
		pluginloader: flowplugin.GoPluginLoader,
	}

	return p, nil
}

func (p *Pipeline) Start() error {
	actors, err := p.topology.Sort()
	if err != nil {
		return err
	}
	for _, actor := range actors {
		p.ctx.Logger().Trace("starting %s", actor.(core.Actor).Name())
		if err := actor.(core.Actor).Start(); err != nil {
			return err
		}
	}

	return nil
}

func (p *Pipeline) Stop() error {
	actors, err := p.topology.Sort()
	if err != nil {
		return err
	}
	l := len(actors)
	for i := 0; i < l/2; i++ {
		actors[i], actors[l-i-1] = actors[l-i-1], actors[i]
	}
	for _, actor := range actors {
		p.ctx.Logger().Trace("stopping %s", actor.(core.Actor).Name())
		if err := actor.(core.Actor).Stop(); err != nil {
			return err
		}
	}

	return nil
}

func (p *Pipeline) Context() *core.Context {
	return p.ctx
}

func buildActors(ctx *core.Context, builders map[string]core.Builder, pl flowplugin.Loader) (map[string]core.Actor, error) {
	actblocks, ok := ctx.Config().Get(types.NewKey("actors"))
	if !ok {
		return nil, fmt.Errorf("`actors` config is missing")
	}
	actors := make(map[string]core.Actor)
	for name, actorcfg := range actblocks.(map[string]types.CfgBlockActor) {
		var actor core.Actor
		var err error
		module := actorcfg.Module
		if strings.HasPrefix(module, "core.") {
			actor, err = buildCoreActor(builders, name, ctx, &actorcfg)
		} else if strings.HasPrefix(module, "plugin.") {
			actor, err = buildPluginActor(pl, name, ctx, &actorcfg)
		} else {
			err = fmt.Errorf("unknown actor module: %s", module)
		}
		if err != nil {
			return nil, err
		}
		actors[name] = actor
	}

	return actors, nil
}

func buildCoreActor(builders map[string]core.Builder, name string, ctx *core.Context, cfg *types.CfgBlockActor) (core.Actor, error) {
	module := cfg.Module
	if _, ok := builders[module]; !ok {
		return nil, fmt.Errorf("unrecognised core module: %s", module)
	}
	return (builders[module])(name, ctx, core.Params(cfg.Params))
}

func buildPluginActor(pl flowplugin.Loader, name string, ctx *core.Context, cfg *types.CfgBlockActor) (core.Actor, error) {
	pname := strings.Replace(cfg.Module, "plugin.", "", 1)

	ctx.Logger().Debug("initializing plugin %q", pname)

	ppath, ok := ctx.Config().Get(types.NewKey("plugin.path"))
	if !ok {
		return nil, fmt.Errorf("failed to get `plugin.path` config")
	}
	fullpath := path.Join(ppath.(string), pname, pname+".so")

	ctx.Logger().Trace("loading plugin shared library: %s", fullpath)

	plugin, err := pl(fullpath)
	if err != nil {
		return nil, err
	}

	ctx.Logger().Trace("successfully loaded plugin %q shared library", pname)

	ctx.Logger().Trace("searching for plugin %q constructor: %q", pname, cfg.Builder)
	c, err := plugin.Lookup(cfg.Builder)
	if err != nil {
		return nil, err
	}
	ctx.Logger().Trace("successfully loaded plugin %q constructor", pname)

	return c.(func(string, *core.Context, core.Params) (core.Actor, error))(name, ctx, core.Params(cfg.Params))
}

func buildTopology(ctx *core.Context, actors map[string]core.Actor) (*data.Topology, error) {
	topology := data.NewTopology()
	for _, actor := range actors {
		topology.AddNode(actor)
	}

	pipeline, ok := ctx.Config().Get(types.NewKey("pipeline"))
	if !ok {
		return nil, fmt.Errorf("pipeline config is missing")
	}

	nthreads, _ := ctx.Config().Get(types.NewKey("system.maxprocs"))

	for name, cfg := range pipeline.(map[string]types.CfgBlockPipeline) {
		actor, ok := actors[name]
		if !ok {
			return nil, fmt.Errorf("unknown actor in the pipeline config: %s", name)
		}
		if len(cfg.Connect) != 0 {
			for _, connect := range cfg.Connect {
				peer, ok := actors[connect]
				if !ok {
					return nil, fmt.Errorf("unknown peer in the pipeline config: %s", cfg.Connect)
				}
				if err := actor.Connect(nthreads.(int), peer); err != nil {
					return nil, err
				}
				if err := topology.Connect(actor, peer); err != nil {
					return nil, err
				}
			}
		}
	}

	return topology, nil
}
