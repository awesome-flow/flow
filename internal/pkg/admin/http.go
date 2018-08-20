package admin

import "github.com/whiteboxio/flow/pkg/config"

type HTTP struct{}

func NewHTTP(cfg *config.CfgBlockSystem) (*HTTP, error) {
	return &HTTP{}, nil
}
