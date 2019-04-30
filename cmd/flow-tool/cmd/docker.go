package cmd

import (
	"fmt"
	"io/ioutil"
	"os"

	"github.com/spf13/cobra"
	"gopkg.in/yaml.v2"

	"github.com/awesome-flow/flow/pkg/cfg"
	"github.com/awesome-flow/flow/pkg/devenv"
	"github.com/awesome-flow/flow/pkg/pipeline"
	"github.com/awesome-flow/flow/pkg/types"
)

var dockerCmd = &cobra.Command{
	Use:   "docker [command]",
	Short: "Docker dev env related toolkit",
}

var flowconfig string
var outfile string
var pluginpath string
var pluginpathprov cfg.Provider
var flowconfigprov cfg.Provider

type SimpleProv struct {
	k types.Key
	v types.Value
}

var _ cfg.Provider = (*SimpleProv)(nil)

func NewSimpleProv(k types.Key, v types.Value) *SimpleProv {
	return &SimpleProv{k: k, v: v}
}

func (sp *SimpleProv) Get(k types.Key) (*types.KeyValue, bool) {
	if k.String() == sp.k.String() {
		return &types.KeyValue{Key: sp.k, Value: sp.v}, true
	}
	return nil, false
}

func (*SimpleProv) Depends() []string              { return nil }
func (*SimpleProv) Weight() int                    { return 42 }
func (sp *SimpleProv) Name() string                { return fmt.Sprintf("Simple Provider [%q]", sp.k.String()) }
func (*SimpleProv) SetUp(*cfg.Repository) error    { return nil }
func (*SimpleProv) TearDown(*cfg.Repository) error { return nil }

var repo *cfg.Repository

func init() {
	repo = cfg.NewRepository()
	repo.RegisterKey(types.NewKey("plugin.path"), NewSimpleProv(types.NewKey("plugin.path"), pluginpath))
	repo.RegisterKey(types.NewKey("config.path"), NewSimpleProv(types.NewKey("config.path"), flowconfig))
	if err := repo.SetUp(); err != nil {
		panic(fmt.Sprintf("Failed to set up repo: %s", err))
	}
}

var dockerComposeCmd = &cobra.Command{
	Use:   "compose",
	Short: "Generates docker-compose file from the pipeline",
	Long: `This command scaffolds a docker-compose file based
		on flow config file. Flow links, including plugins,
		can define their dev environment using docker-compose
		yaml. This definition is being picked up by flow-tool
		and compiled into a single docker-compose file. Links
		can template their definition parameters (names, ports)
		in order to not interfere with their siblings.`,
	RunE: func(cmd *cobra.Command, args []string) error {

		data, err := ioutil.ReadFile(flowconfig)
		if err != nil {
			return err
		}

		var yamlcfg types.Cfg
		if err := yaml.Unmarshal(data, &yamlcfg); err != nil {
			return nil
		}

		pipeline, err := pipeline.NewPipeline(yamlcfg.Components, yamlcfg.Pipeline)
		if err != nil {
			return err
		}

		var devctx devenv.Context
		dockercompfrags := make([]devenv.DockerComposeFragment, 0)
		for _, link := range pipeline.Links() {
			fragments, err := link.DevEnv(&devctx)
			if err != nil {
				return err
			}
			for _, fragment := range fragments {
				if dockercompfrag, ok := fragment.(devenv.DockerComposeFragment); ok {
					dockercompfrags = append(dockercompfrags, dockercompfrag)
				}
			}
		}

		dockercomp, err := devenv.DockerComposeBuilder(dockercompfrags)
		if err != nil {
			return err
		}

		var out *os.File
		if outfile == "" {
			out = os.Stdout
		} else {
			out, err = os.OpenFile(outfile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
			if err != nil {
				return err
			}
		}
		defer out.Close()

		if _, err := out.WriteString(dockercomp); err != nil {
			return err
		}

		return nil
	},
}

func init() {
	dockerCmd.AddCommand(dockerComposeCmd)
	rootCmd.AddCommand(dockerCmd)

	dockerComposeCmd.Flags().StringVarP(&flowconfig, "flow-config", "c", "", "Source YAML flowd config")
	dockerComposeCmd.Flags().StringVarP(&outfile, "out", "o", "", "Output to file (STDOUT by default")
	dockerComposeCmd.Flags().StringVarP(&pluginpath, "plugin-path", "p", "", "Flow plugin path")
	dockerComposeCmd.MarkFlagRequired("flow-config")
}
