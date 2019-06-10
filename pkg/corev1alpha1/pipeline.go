package corev1alpha1

import (
	"fmt"

	"github.com/awesome-flow/flow/pkg/types"
	"github.com/awesome-flow/flow/pkg/util/data"
)

type Pipeline struct {
	ctx      *Context
	actors   map[string]Actor
	topology *data.Topology
}

var _ Runner = (*Pipeline)(nil)

func NewPipeline(ctx *Context) (*Pipeline, error) {
	actors, err := buildActors(ctx)
	if err != nil {
		return nil, err
	}

	topology, err := buildTopology(ctx)
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
	return nil
}

func (p *Pipeline) Stop() error {
	return nil
}

func buildActors(ctx *Context) (map[string]Actor, error) {
	comps, ok := ctx.config.Repo().Get(types.NewKey("components"))
	if !ok {
		return nil, fmt.Errorf("Components config is missing")
	}
	for name, actorcfg := range comps.(map[string]*types.CfgBlockComponent) {
		//TODO
	}

	return make(map[string]Actor), nil
}

func buildTopology(ctx *Context) (*data.Topology, error) {
	return data.NewTopology(), nil
}
