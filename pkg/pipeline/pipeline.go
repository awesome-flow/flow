package pipeline

import (
	"fmt"
	"math/rand"
	"plugin"
	"runtime"
	"time"

	log "github.com/sirupsen/logrus"

	"github.com/awesome-flow/flow/pkg/config"
	"github.com/awesome-flow/flow/pkg/core"
	"github.com/awesome-flow/flow/pkg/util/data"

	evio_rcv "github.com/awesome-flow/flow/pkg/receiver/evio"
	http_rcv "github.com/awesome-flow/flow/pkg/receiver/http"
	tcp_rcv "github.com/awesome-flow/flow/pkg/receiver/tcp"
	udp_rcv "github.com/awesome-flow/flow/pkg/receiver/udp"
	unix_rcv "github.com/awesome-flow/flow/pkg/receiver/unix"

	buffer "github.com/awesome-flow/flow/pkg/link/buffer"
	compressor "github.com/awesome-flow/flow/pkg/link/compressor"
	demux "github.com/awesome-flow/flow/pkg/link/demux"
	fanout "github.com/awesome-flow/flow/pkg/link/fanout"
	meta_parser "github.com/awesome-flow/flow/pkg/link/meta_parser"
	mux "github.com/awesome-flow/flow/pkg/link/mux"
	replicator "github.com/awesome-flow/flow/pkg/link/replicator"
	router "github.com/awesome-flow/flow/pkg/link/router"
	throttler "github.com/awesome-flow/flow/pkg/link/throttler"

	dumper_sink "github.com/awesome-flow/flow/pkg/sink/dumper"
	null_sink "github.com/awesome-flow/flow/pkg/sink/null"
	tcp_sink "github.com/awesome-flow/flow/pkg/sink/tcp"
	udp_sink "github.com/awesome-flow/flow/pkg/sink/udp"
)

type Pipeline struct {
	pplCfg   map[string]config.CfgBlockPipeline
	compsCfg map[string]config.CfgBlockComponent
	compTree *data.NTree
}

type ConstrFunc func(string, core.Params, *core.Context) (core.Link, error)

var (
	compBuilders = map[string]ConstrFunc{
		"receiver.tcp":  tcp_rcv.New,
		"receiver.udp":  udp_rcv.New,
		"receiver.http": http_rcv.New,
		"receiver.unix": unix_rcv.New,
		"receiver.evio": evio_rcv.New,

		"link.demux":       demux.New,
		"link.mux":         mux.New,
		"link.router":      router.New,
		"link.throttler":   throttler.New,
		"link.fanout":      fanout.New,
		"link.replicator":  replicator.New,
		"link.buffer":      buffer.New,
		"link.meta_parser": meta_parser.New,
		"link.compressor":  compressor.New,

		"sink.dumper": dumper_sink.New,
		"sink.tcp":    tcp_sink.New,
		"sink.udp":    udp_sink.New,
		"sink.null":   null_sink.New,
	}
)

func NewPipeline(
	compsCfg map[string]config.CfgBlockComponent,
	pplCfg map[string]config.CfgBlockPipeline) (*Pipeline, error) {

	compPool := make(map[string]core.Link)
	for compName, compParams := range compsCfg {
		context := core.NewContext()
		comp, compErr := buildComp(compName, compParams, context)
		if compErr != nil {
			return nil, compErr
		}
		if _, ok := compPool[compName]; ok {
			return nil,
				fmt.Errorf(
					"Duplicate declaration of component called %s", compName)
		}
		compPool[compName] = comp
	}

	for compName, compCfg := range pplCfg {
		comp, ok := compPool[compName]
		if !ok {
			return nil, fmt.Errorf(
				"Undefined component %s in the pipeline", compName)
		}
		if compCfg.Connect != "" {
			log.Infof("Connecting %s to %s", compName, compCfg.Connect)
			if _, ok := compPool[compCfg.Connect]; !ok {
				return nil, fmt.Errorf(
					"Failed to connect %s to %s: %s is undefined",
					compName, compCfg.Connect, compCfg.Connect)
			}
			if err := comp.ConnectTo(compPool[compCfg.Connect]); err != nil {
				return nil, fmt.Errorf("Failed to connect %s to %s: %s",
					compName, compCfg.Connect, err.Error())
			}
		}
		if len(compCfg.Links) > 0 {
			log.Infof("Linking %s with %s", compName, compCfg.Links)
			links := make([]core.Link, len(compCfg.Links))
			for ix, linkName := range compCfg.Links {
				if _, ok := compPool[linkName]; !ok {
					return nil, fmt.Errorf(
						"Failed to link %s to %s: %s is undefined",
						compName, linkName, linkName)
				}
				links[ix] = compPool[linkName]
			}
			if err := comp.LinkTo(links); err != nil {
				return nil, fmt.Errorf(
					"Failed to link %s: %s", compName, err.Error())
			}
		}
		if len(compCfg.Routes) > 0 {
			routes := make(map[string]core.Link)
			for rtPath, rtName := range compCfg.Routes {
				if _, ok := compPool[rtName]; !ok {
					return nil, fmt.Errorf(
						"Failed to route %s to %s under path %s: %s is undefined",
						compName, rtName, rtPath, rtName)
				}
				routes[rtPath] = compPool[rtName]
			}
			if err := comp.RouteTo(routes); err != nil {
				return nil, fmt.Errorf("Failed to route %s: %s",
					compName, err.Error())
			}
		}
	}

	pipeline := &Pipeline{
		pplCfg:   pplCfg,
		compsCfg: compsCfg,
		compTree: buildCompTree(pplCfg, compPool),
	}

	pipeline.applySysCfg()

	return pipeline, nil
}

func buildComp(compName string, cfg config.CfgBlockComponent, context *core.Context) (core.Link, error) {
	if cfg.Plugin != "" {
		pluginPath, _ := config.Get("flow.plugin.path")
		p, pErr := plugin.Open(fmt.Sprintf("%s/%s/%s.so",
			pluginPath.(string), cfg.Plugin, cfg.Plugin))
		if pErr != nil {
			return nil, pErr
		}
		c, cErr := p.Lookup(cfg.Constructor)
		if cErr != nil {
			return nil, cErr
		}
		comp, err := c.(func(string, core.Params, *core.Context) (core.Link, error))(compName, cfg.Params, context)
		if err != nil {
			panic(err.Error())
		}
		if comp == nil {
			panic("Component is nil")
		}
		return comp, err
	}
	if builder, ok := compBuilders[cfg.Module]; ok {
		return builder(compName, cfg.Params, context)
	}
	return nil, fmt.Errorf("Unknown module: %s requested by %s", cfg.Module, compName)
}

func (ppl *Pipeline) Explain() (string, error) {
	dotexplain := &DotExplainer{}
	return dotexplain.Explain(ppl)
}

func (ppl *Pipeline) Links() []core.Link {
	links := make([]core.Link, 0)
	for _, link := range ppl.compTree.PreTraversal() {
		links = append(links, link.(core.Link))
	}
	return links
}

func (ppl *Pipeline) ExecCmd(cmd *core.Cmd, cmdPpgt core.CmdPropagation) error {
	var stack []interface{}
	switch cmdPpgt {
	case core.CmdPpgtBtmUp:
		stack = ppl.compTree.PostTraversal()
	case core.CmdPpgtTopDwn:
		stack = ppl.compTree.PreTraversal()
	default:
		panic("This should not happen")
	}
	for _, link := range stack {
		if err := link.(core.Link).ExecCmd(cmd); err != nil {
			return err
		}
	}
	return nil
}

func (ppl *Pipeline) Start() error {
	rand.Seed(time.Now().UTC().UnixNano())
	return ppl.ExecCmd(&core.Cmd{Code: core.CmdCodeStart}, core.CmdPpgtBtmUp)
}

func (ppl *Pipeline) Stop() error {
	return ppl.ExecCmd(&core.Cmd{Code: core.CmdCodeStop}, core.CmdPpgtTopDwn)
}

func (ppl *Pipeline) applySysCfg() error {
	sysCfgItf, ok := config.Get("global.system")
	if !ok {
		log.Infof("The pipeline is being initialized with default system settings")
		return nil
	}
	sysCfg, convOk := sysCfgItf.(*config.CfgBlockSystem)
	if !convOk {
		err := fmt.Errorf("Failed to convert sysCfg to *config.CfgBlockSystem")
		return err
	}

	log.Infof("Setting GOMAXPROCS to %d", sysCfg.Maxprocs)
	runtime.GOMAXPROCS(sysCfg.Maxprocs)

	return nil
}

func buildCompTree(ppl map[string]config.CfgBlockPipeline,
	lookup map[string]core.Link) *data.NTree {

	rootNode := &data.NTree{}

	for name, block := range ppl {
		ptr := rootNode.FindOrInsert(lookup[name])
		children := make([]core.Link, 0)
		if len(block.Connect) > 0 {
			children = append(children, lookup[block.Connect])
		}
		if len(block.Links) > 0 {
			for _, linkName := range block.Links {
				children = append(children, lookup[linkName])
			}
		}
		if len(block.Routes) > 0 {
			for _, routeName := range block.Routes {
				children = append(children, lookup[routeName])
			}
		}
		for _, chld := range children {
			if chldPtr := rootNode.Detach(chld); chldPtr != nil {
				ptr.FindOrInsert(chldPtr.GetValue())
			}
			ptr.FindOrInsert(chld)
		}
	}

	return rootNode
}
