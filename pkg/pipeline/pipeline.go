package pipeline

import (
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
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
	compTop  *data.Topology
}

type Constructor func(string, core.Params, *core.Context) (core.Link, error)

var (
	CompBuilders = map[string]Constructor{
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

func buildComponents(cfg map[string]config.CfgBlockComponent) (map[string]core.Link, error) {
	components := make(map[string]core.Link)
	for name, params := range cfg {
		ctx := core.NewContext()
		if _, ok := components[name]; ok {
			return nil, fmt.Errorf("Duplicate declaration of component %q", name)
		}
		comp, err := buildComponent(name, params, ctx)
		if err != nil {
			return nil, err
		}

		components[name] = comp
	}

	return components, nil
}

func NewPipeline(
	compsCfg map[string]config.CfgBlockComponent,
	pplCfg map[string]config.CfgBlockPipeline) (*Pipeline, error) {

	components, err := buildComponents(compsCfg)
	if err != nil {
		return nil, err
	}

	for compName, compCfg := range pplCfg {
		comp, ok := components[compName]
		if !ok {
			return nil, fmt.Errorf(
				"Pipeline component %q mentioned in the pipeline config but never defined in components section", compName)
		}
		if compCfg.Connect != "" {
			log.Infof("Connecting %s to %s", compName, compCfg.Connect)
			if _, ok := components[compCfg.Connect]; !ok {
				return nil, fmt.Errorf(
					"Failed to connect %s to %s: %s is undefined",
					compName, compCfg.Connect, compCfg.Connect)
			}
			if err := comp.ConnectTo(components[compCfg.Connect]); err != nil {
				return nil, fmt.Errorf("Failed to connect %s to %s: %s",
					compName, compCfg.Connect, err.Error())
			}
		}
		if len(compCfg.Links) > 0 {
			log.Infof("Linking %s with %s", compName, compCfg.Links)
			links := make([]core.Link, len(compCfg.Links))
			for ix, linkName := range compCfg.Links {
				if _, ok := components[linkName]; !ok {
					return nil, fmt.Errorf(
						"Failed to link %s to %s: %s is undefined",
						compName, linkName, linkName)
				}
				links[ix] = components[linkName]
			}
			if err := comp.LinkTo(links); err != nil {
				return nil, fmt.Errorf(
					"Failed to link %s: %s", compName, err.Error())
			}
		}
		if len(compCfg.Routes) > 0 {
			routes := make(map[string]core.Link)
			for rtPath, rtName := range compCfg.Routes {
				if _, ok := components[rtName]; !ok {
					return nil, fmt.Errorf(
						"Failed to route %s to %s under path %s: %s is undefined",
						compName, rtName, rtPath, rtName)
				}
				routes[rtPath] = components[rtName]
			}
			if err := comp.RouteTo(routes); err != nil {
				return nil, fmt.Errorf("Failed to route %s: %s",
					compName, err.Error())
			}
		}
	}

	topology, err := buildPipelineTopology(pplCfg, components)
	if err != nil {
		return nil, err
	}

	pipeline := &Pipeline{
		pplCfg:   pplCfg,
		compsCfg: compsCfg,
		compTop:  topology,
	}

	pipeline.applySysCfg()

	return pipeline, nil
}

func componentIsPlugin(cfg config.CfgBlockComponent) bool {
	return len(cfg.Plugin) > 0
}

func buildComponent(compName string, cfg config.CfgBlockComponent, context *core.Context) (core.Link, error) {
	if componentIsPlugin(cfg) {
		return buildPlugin(compName, cfg, context)
	}
	if builder, ok := CompBuilders[cfg.Module]; ok {
		return builder(compName, cfg.Params, context)
	}
	return nil, fmt.Errorf("Unknown module: %s requested by %s", cfg.Module, compName)
}
func buildPlugin(name string, cfg config.CfgBlockComponent, context *core.Context) (core.Link, error) {
	if cfg.Plugin == "" {
		return nil, fmt.Errorf("%q config does not look like a plugin", name)
	}
	var basepath string
	v, ok := config.Get("flow.plugin.path")
	if !ok {
		return nil, fmt.Errorf("Config is missing flow.plugin.path")
	}
	if str, ok := v.(string); ok {
		basepath = str
	} else if strptr, ok := v.(*string); ok {
		basepath = *strptr
	} else {
		return nil, fmt.Errorf("flow.plugin.path is not a string value")
	}
	// /plugin_base/path/plugin_name/plugin_name.so
	fullpath := filepath.Join(basepath, cfg.Plugin, fmt.Sprintf("%s.so", cfg.Plugin))
	log.Debugf("Initializing plugin %q from path: %s", cfg.Plugin, fullpath)

	if _, err := os.Stat(fullpath); os.IsNotExist(err) {
		return nil, fmt.Errorf("Unable to find plugin shared library object under path: %s", fullpath)
	}
	pl, err := plugin.Open(fullpath)
	if err != nil {
		return nil, err
	}
	log.Debugf("Successfully red plugin %q shared library object. Looking up for constructor function %q", cfg.Plugin, cfg.Constructor)

	cnstr, err := pl.Lookup(cfg.Constructor)
	if err != nil {
		return nil, fmt.Errorf("Failed to find the declared constructor function %q for plugin %s: %s", cfg.Constructor, cfg.Plugin, err)
	}

	lnk, err := cnstr.(func(string, core.Params, *core.Context) (core.Link, error))(name, cfg.Params, context)
	if err != nil {
		return nil, err
	}

	if lnk == nil {
		return nil, fmt.Errorf("Plugin %s constructor %s returned a nil object an no error", cfg.Plugin, cfg.Constructor)
	}

	return lnk, nil
}

func (ppl *Pipeline) Explain() (string, error) {
	dotexplain := &DotExplainer{}
	return dotexplain.Explain(ppl)
}

func (ppl *Pipeline) Links() []core.Link {
	sorted, err := ppl.compTop.Sort()
	if err != nil {
		panic(err.Error())
	}
	for i := 0; i < len(sorted)/2; i++ {
		sorted[i], sorted[len(sorted)-1-i] = sorted[len(sorted)-1-i], sorted[i]
	}
	links := make([]core.Link, 0, len(sorted))
	for _, node := range links {
		links = append(links, node.(core.Link))
	}
	return links
}

func (ppl *Pipeline) ExecCmd(cmd *core.Cmd, cmdPpgt core.CmdPropagation) error {
	sorted, err := ppl.compTop.Sort()
	if err != nil {
		return err
	}
	switch cmdPpgt {
	case core.CmdPpgtTopDwn:
		l := len(sorted)
		for i := 0; i < l/2; i++ {
			sorted[i], sorted[l-1-i] = sorted[l-1-i], sorted[i]
		}
	case core.CmdPpgtBtmUp:
	default:
		return fmt.Errorf("Unknown command propagation: %d", cmdPpgt)
	}

	for _, topNode := range sorted {
		if err := topNode.(core.Link).ExecCmd(cmd); err != nil {
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

func buildPipelineTopology(cfg map[string]config.CfgBlockPipeline,
	components map[string]core.Link) (*data.Topology, error) {
	top := data.NewTopology()

	for _, component := range components {
		top.AddNode(component)
	}

	for name, blockcfg := range cfg {
		hasConnection := blockHasConnection(blockcfg)
		hasLinks := blockHasLinks(blockcfg)
		hasRoutes := blockHasRoutes(blockcfg)
		if hasConnection {
			connectTo, ok := components[blockcfg.Connect]
			if !ok {
				return nil, fmt.Errorf(
					"Component %q defined a connection to an unknown component %q",
					name,
					blockcfg.Connect)
			}
			top.Connect(components[name], connectTo)
		}
		if hasLinks {
			for _, linkName := range blockcfg.Links {
				linkTo, ok := components[linkName]
				if !ok {
					return nil, fmt.Errorf(
						"Component %q defined a link to an unknown component %q",
						name,
						linkName)
				}
				if hasConnection {
					// Link is incoming is connectTo is defined
					top.Connect(linkTo, components[name])
				} else {
					// Link is outcoming otherwise
					top.Connect(components[name], linkTo)
				}
			}
		}
		if hasRoutes {
			for _, routeName := range blockcfg.Routes {
				routeTo, ok := components[routeName]
				if !ok {
					return nil, fmt.Errorf(
						"Component %q defined a route to an unknown component %q",
						name,
						routeName)
				}
				top.Connect(components[name], routeTo)
			}
		}
	}

	return top, nil
}

func blockHasConnection(blockcfg config.CfgBlockPipeline) bool {
	return len(blockcfg.Connect) > 0
}

func blockHasLinks(blockcfg config.CfgBlockPipeline) bool {
	return len(blockcfg.Links) > 0
}

func blockHasRoutes(blockcfg config.CfgBlockPipeline) bool {
	return len(blockcfg.Routes) > 0
}
