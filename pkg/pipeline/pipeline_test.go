package pipeline

import (
	"fmt"
	"testing"

	"github.com/whiteboxio/flow/pkg/config"
	"github.com/whiteboxio/flow/pkg/core"
)

func TestPipeline_buildComp(t *testing.T) {
	tests := []struct {
		name string
		cfg  *config.CfgBlockComponent
	}{
		{
			"TCP receiver",
			&config.CfgBlockComponent{
				Module: "receiver.tcp",
				Params: core.Params{"bind_addr": ":13101"},
			},
		},
		{
			"UDP receiver",
			&config.CfgBlockComponent{
				Module: "receiver.udp",
				Params: core.Params{"bind_addr": ":13102"},
			},
		},
		{
			"HTTP receiver",
			&config.CfgBlockComponent{
				Module: "receiver.http",
				Params: core.Params{"bind_addr": ":13103"},
			},
		},
		{
			"Unix receiver",
			&config.CfgBlockComponent{
				Module: "receiver.unix",
				Params: core.Params{"path": "/tmp/flow.sock.test"},
			},
		},
		{
			"DMX link",
			&config.CfgBlockComponent{
				Module: "link.dmx",
				Params: core.Params{},
			},
		},
		{
			"MPX link",
			&config.CfgBlockComponent{
				Module: "link.fanout",
				Params: core.Params{},
			},
		},
		{
			"Router link",
			&config.CfgBlockComponent{
				Module: "link.router",
				Params: core.Params{"routing_key": "type"},
			},
		},
		{
			"Throttler link",
			&config.CfgBlockComponent{
				Module: "link.throttler",
				Params: core.Params{"rps": 42},
			},
		},
		{
			"Dumper sink",
			&config.CfgBlockComponent{
				Module: "sink.dumper",
				Params: core.Params{"out": "/dev/null"},
			},
		},
		{
			"TCP sink",
			&config.CfgBlockComponent{
				Module: "sink.tcp",
				Params: core.Params{"bind_addr": ":13101"},
			},
		},
		{
			"UDP sink",
			&config.CfgBlockComponent{
				Module: "sink.udp",
				Params: core.Params{"bind_addr": "13102"},
			},
		},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			comp, err := buildComp(testCase.cfg.Module, *testCase.cfg)
			if err != nil {
				t.Errorf("Failed to build component %s: %s", testCase.cfg.Module, err)
			} else {
				fmt.Printf("Closing %s\n", testCase.cfg.Module)
				comp.ExecCmd(&core.Cmd{Code: core.CmdCodeStop})
			}
		})
	}
}
