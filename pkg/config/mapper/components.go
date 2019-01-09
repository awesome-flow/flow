package config_mapper

import (
	"fmt"

	"github.com/awesome-flow/flow/pkg/config"
)

func GetComponentsCfg() (map[string]config.CfgBlockComponent, error) {
	vIntf, ok := config.Get(config.YML_CFG_KEY_COMP)
	if !ok {
		return nil, fmt.Errorf("Missing components config block")
	}
	v, convOk := vIntf.(map[string]config.CfgBlockComponent)
	if !convOk {
		return nil, fmt.Errorf(
			"Malformed components config type: expected: map[string]config_mappers.CfgBlockComponents, got: %+v", vIntf)
	}
	return v, nil
}
