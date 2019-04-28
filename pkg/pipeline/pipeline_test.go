package pipeline

import (
	"fmt"
	"testing"

	"github.com/awesome-flow/flow/pkg/cast"
	"github.com/awesome-flow/flow/pkg/core"
	yaml "gopkg.in/yaml.v2"
)

func TestPipeline_buildComponents(t *testing.T) {
	tests := []struct {
		name string
		cfg  *cast.CfgBlockComponent
	}{
		{
			"TCP receiver",
			&cast.CfgBlockComponent{
				Module: "receiver.tcp",
				Params: core.Params{"bind_addr": ":13101"},
			},
		},
		{
			"UDP receiver",
			&cast.CfgBlockComponent{
				Module: "receiver.udp",
				Params: core.Params{"bind_addr": ":13102"},
			},
		},
		{
			"HTTP receiver",
			&cast.CfgBlockComponent{
				Module: "receiver.http",
				Params: core.Params{"bind_addr": ":13103"},
			},
		},
		{
			"Unix receiver",
			&cast.CfgBlockComponent{
				Module: "receiver.unix",
				Params: core.Params{"path": "/tmp/flow.sock.test"},
			},
		},
		{
			"Demux link",
			&cast.CfgBlockComponent{
				Module: "link.demux",
				Params: core.Params{},
			},
		},
		{
			"Mux link",
			&cast.CfgBlockComponent{
				Module: "link.mux",
				Params: core.Params{},
			},
		},
		{
			"Router link",
			&cast.CfgBlockComponent{
				Module: "link.router",
				Params: core.Params{"routing_key": "type"},
			},
		},
		{
			"Throttler link",
			&cast.CfgBlockComponent{
				Module: "link.throttler",
				Params: core.Params{"rps": 42},
			},
		},
		{
			"Dumper sink",
			&cast.CfgBlockComponent{
				Module: "sink.dumper",
				Params: core.Params{"out": "/dev/null"},
			},
		},
		{
			"TCP sink",
			&cast.CfgBlockComponent{
				Module: "sink.tcp",
				Params: core.Params{"bind_addr": ":13101"},
			},
		},
		{
			"UDP sink",
			&cast.CfgBlockComponent{
				Module: "sink.udp",
				Params: core.Params{"bind_addr": ":13102"},
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

func cfgFromYaml(body []byte) (*cast.Cfg, error) {
	cfg := &cast.Cfg{}
	err := yaml.Unmarshal(body, cfg)
	if err != nil {
		return nil, err
	}
	return cfg, nil
}
