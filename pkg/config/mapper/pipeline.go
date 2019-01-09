package config_mapper

import (
	"fmt"

	"github.com/awesome-flow/flow/pkg/config"
)

func GetPipelineCfg() (map[string]config.CfgBlockPipeline, error) {
	vIntf, ok := config.Get(config.YML_CFG_KEY_PPL)
	if !ok {
		return nil, fmt.Errorf("Missing pipeline config block")
	}
	v, convOk := vIntf.(map[string]config.CfgBlockPipeline)
	if !convOk {
		return nil, fmt.Errorf(
			"Malformed pipeline config type: expected: map[string]config_mappers.CfgBlockPipeline, got: %+v", vIntf)
	}
	return v, nil
}
