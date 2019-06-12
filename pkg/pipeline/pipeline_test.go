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
		cfg  *types.CfgBlockActor
	}{
		{
			"TCP receiver",
			&types.CfgBlockActor{
				Module: "receiver.tcp",
				Params: types.Params{"bind_addr": ":13101"},
			},
		},
		{
			"UDP receiver",
			&types.CfgBlockActor{
				Module: "receiver.udp",
				Params: types.Params{"bind_addr": ":13102"},
			},
		},
		{
			"HTTP receiver",
			&types.CfgBlockActor{
				Module: "receiver.http",
				Params: types.Params{"bind_addr": ":13103"},
			},
		},
		{
			"Unix receiver",
			&types.CfgBlockActor{
				Module: "receiver.unix",
				Params: types.Params{"path": "/tmp/flow.sock.test"},
			},
		},
		{
			"Demux link",
			&types.CfgBlockActor{
				Module: "link.demux",
				Params: types.Params{},
			},
		},
		{
			"Mux link",
			&types.CfgBlockActor{
				Module: "link.mux",
				Params: types.Params{},
			},
		},
		{
			"Router link",
			&types.CfgBlockActor{
				Module: "link.router",
				Params: types.Params{"routing_key": "type"},
			},
		},
		{
			"Throttler link",
			&types.CfgBlockActor{
				Module: "link.throttler",
				Params: types.Params{"rps": 42},
			},
		},
		{
			"Dumper sink",
			&types.CfgBlockActor{
				Module: "sink.dumper",
				Params: types.Params{"out": "/dev/null"},
			},
		},
		{
			"TCP sink",
			&types.CfgBlockActor{
				Module: "sink.tcp",
				Params: types.Params{"bind_addr": ":13101"},
			},
		},
		{
			"UDP sink",
			&types.CfgBlockActor{
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
