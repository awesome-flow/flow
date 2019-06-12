package pipeline

import (
	"fmt"
	"strings"

	core "github.com/awesome-flow/flow/pkg/corev1alpha1"
	"github.com/awesome-flow/flow/pkg/corev1alpha1/actor"
	"github.com/awesome-flow/flow/pkg/types"
	"github.com/awesome-flow/flow/pkg/util/data"
)

var builders map[string]core.Constructor = map[string]core.Constructor{
	"core.receiver.tcp":  actor.NewReceiverTCP,
	"core.receiver.udp":  nil,
	"core.receiver.http": actor.NewReceiverHTTP,
	"core.receiver.unix": nil,

	"core.demux":      nil,
	"core.mux":        nil,
	"core.router":     actor.NewRouter,
	"core.throttler":  nil,
	"core.fanout":     nil,
	"core.replicator": nil,
	"core.buffer":     nil,
	"core.compressor": nil,

	"core.sink.dumper": actor.NewSinkDumper,
	"core.sink.tcp":    nil,
	"core.sink.udp":    nil,
	"core.sink.null":   nil,
}

type Pipeline struct {
	ctx      *core.Context
	actors   map[string]core.Actor
	topology *data.Topology
}

var _ core.Runner = (*Pipeline)(nil)

func NewPipeline(ctx *core.Context) (*Pipeline, error) {
	actors, err := buildActors(ctx)
	if err != nil {
		return nil, err
	}

	topology, err := buildTopology(ctx, actors)
	if err != nil {
		return nil, err
	}

	p := &Pipeline{
		ctx:      ctx,
		actors:   actors,
		topology: topology,
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

func buildActors(ctx *core.Context) (map[string]core.Actor, error) {
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
			actor, err = buildCoreActor(name, ctx, &actorcfg)
		} else if strings.HasPrefix(name, "plugin.") {
			actor, err = buildPluginActor(name, ctx, &actorcfg)
		} else {
			err = fmt.Errorf("unknown actor module: %s", name)
		}
		if err != nil {
			return nil, err
		}
		actors[name] = actor
	}

	return actors, nil
}

func buildCoreActor(name string, ctx *core.Context, cfg *types.CfgBlockActor) (core.Actor, error) {
	module := cfg.Module
	if _, ok := builders[module]; !ok {
		return nil, fmt.Errorf("unrecognised core module: %s", module)
	}
	return (builders[module])(name, ctx, core.Params(cfg.Params))
}

func buildPluginActor(name string, ctx *core.Context, cfg *types.CfgBlockActor) (core.Actor, error) {
	//TODO
	return nil, nil
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
		if cfg.Connect != "" {
			peer, ok := actors[cfg.Connect]
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

	return topology, nil
}
