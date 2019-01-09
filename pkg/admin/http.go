package admin

import (
	"fmt"
	"net/http"

	log "github.com/sirupsen/logrus"
	"github.com/awesome-flow/flow/pkg/admin/agent"
	"github.com/awesome-flow/flow/pkg/config"
)

type HTTP struct {
	server *http.Server
}

func newAdminSrvMx(cfg *config.CfgBlockSystem) *http.ServeMux {
	srvMx := http.NewServeMux()

	for _, wa := range agent.AllAgents() {
		srvMx.Handle(wa.GetPath(), wa.GetHandler())
	}

	return srvMx
}

func NewHTTP(cfg *config.CfgBlockSystem) (*HTTP, error) {
	srvMx := newAdminSrvMx(cfg)
	server := &http.Server{
		Addr:    cfg.Admin.BindAddr,
		Handler: srvMx,
	}
	h := &HTTP{server}

	go func() {
		if err := server.ListenAndServe(); err != nil {
			switch err {
			case http.ErrServerClosed:
				log.Info("Admin server closed")
			default:
				log.Errorf(fmt.Sprintf("Admin server critical error: %s", err))
			}
		}
	}()

	return h, nil
}
