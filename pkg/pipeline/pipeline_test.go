package pipeline

import (
	"fmt"
	"testing"

	"github.com/awesome-flow/flow/pkg/core"
	"github.com/awesome-flow/flow/pkg/types"
	yaml "gopkg.in/yaml.v2"
)

func TestPipeline_buildComponents(t *testing.T) {
	tests := []struct {
		name string
		cfg  *types.CfgBlockComponent
	}{
		{
			"TCP receiver",
			&types.CfgBlockComponent{
				Module: "receiver.tcp",
				Params: types.Params{"bind_addr": ":13101"},
			},
		},
		{
			"UDP receiver",
			&types.CfgBlockComponent{
				Module: "receiver.udp",
				Params: types.Params{"bind_addr": ":13102"},
			},
		},
		{
			"HTTP receiver",
			&types.CfgBlockComponent{
				Module: "receiver.http",
				Params: types.Params{"bind_addr": ":13103"},
			},
		},
		{
			"Unix receiver",
			&types.CfgBlockComponent{
				Module: "receiver.unix",
				Params: types.Params{"path": "/tmp/flow.sock.test"},
			},
		},
		{
			"Demux link",
			&types.CfgBlockComponent{
				Module: "link.demux",
				Params: types.Params{},
			},
		},
		{
			"Mux link",
			&types.CfgBlockComponent{
				Module: "link.mux",
				Params: types.Params{},
			},
		},
		{
			"Router link",
			&types.CfgBlockComponent{
				Module: "link.router",
				Params: types.Params{"routing_key": "type"},
			},
		},
		{
			"Throttler link",
			&types.CfgBlockComponent{
				Module: "link.throttler",
				Params: types.Params{"rps": 42},
			},
		},
		{
			"Dumper sink",
			&types.CfgBlockComponent{
				Module: "sink.dumper",
				Params: types.Params{"out": "/dev/null"},
			},
		},
		{
			"TCP sink",
			&types.CfgBlockComponent{
				Module: "sink.tcp",
				Params: types.Params{"bind_addr": ":13101"},
			},
		},
		{
			"UDP sink",
			&types.CfgBlockComponent{
				Module: "sink.udp",
				Params: types.Params{"bind_addr": ":13102"},
			},
		},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			comp, err := buildComponent(testCase.cfg.Module, *testCase.cfg, core.NewContext())
			if err != nil {
				t.Errorf("Failed to build component %s: %s", testCase.cfg.Module, err)
			} else {
				fmt.Printf("Closing %s\n", testCase.cfg.Module)
				comp.ExecCmd(&core.Cmd{Code: core.CmdCodeStop})
			}
		})
	}
}

func cfgFromYaml(body []byte) (*types.Cfg, error) {
	cfg := &types.Cfg{}
	err := yaml.Unmarshal(body, cfg)
	if err != nil {
		return nil, err
	}
	return cfg, nil
}
