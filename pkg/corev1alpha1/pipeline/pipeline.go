package pipeline

import (
	"fmt"
	"strings"

	core "github.com/awesome-flow/flow/pkg/corev1alpha1"
	"github.com/awesome-flow/flow/pkg/types"
	"github.com/awesome-flow/flow/pkg/util/data"
)

type Pipeline struct {
	ctx       *core.Context
	actors    map[string]core.Actor
	topology  *data.Topology
	factories map[string]ActorFactory
}

var _ core.Runner = (*Pipeline)(nil)

func NewPipeline(ctx *core.Context) (*Pipeline, error) {
	return NewPipelineWithFactories(
		ctx,
		map[string]ActorFactory{
			"core":   NewCoreActorFactory(),
			"plugin": NewPluginActorFactory(),
		},
	)
}

func NewPipelineWithFactories(ctx *core.Context, factories map[string]ActorFactory) (*Pipeline, error) {
	actors, err := buildActors(ctx, factories)
	if err != nil {
		return nil, err
	}

	topology, err := buildTopology(ctx, actors)
	if err != nil {
		return nil, err
	}

	p := &Pipeline{
		ctx:       ctx,
		actors:    actors,
		topology:  topology,
		factories: factories,
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

func buildActors(ctx *core.Context, factories map[string]ActorFactory) (map[string]core.Actor, error) {
	actblocks, ok := ctx.Config().Get(types.NewKey("actors"))
	if !ok {
		return nil, fmt.Errorf("`actors` config is missing")
	}
	actors := make(map[string]core.Actor)

	for name, actorcfg := range actblocks.(map[string]types.CfgBlockActor) {
		module := actorcfg.Module

		factkey := strings.Split(module, ".")[0]
		if len(factkey) == 0 {
			factkey = module
		}

		if _, ok := factories[factkey]; !ok {
			return nil, fmt.Errorf("failed to find an actor factory for key %s", factkey)
		}

		actor, err := factories[factkey].Build(name, ctx, &actorcfg)
		if err != nil {
			return nil, err
		}

		actors[name] = actor
	}

	return actors, nil
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
