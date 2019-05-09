package app

import (
	"fmt"
	"net/http"

	"github.com/awesome-flow/flow/pkg/types"
	"github.com/awesome-flow/flow/web/app/agent"
	log "github.com/sirupsen/logrus"
)

type HttpMux struct {
	server *http.Server
}

func newAdminSrvMx(cfg *types.CfgBlockSystem) *http.ServeMux {
	srvMx := http.NewServeMux()

	for _, wa := range agent.AllAgents() {
		srvMx.Handle(wa.GetPath(), wa.GetHandler())
	}

	return srvMx
}

func NewHttpMux(cfg *types.CfgBlockSystem) (*HttpMux, error) {
	srvMx := newAdminSrvMx(cfg)
	server := &http.Server{
		Addr:    cfg.Admin.BindAddr,
		Handler: srvMx,
	}
	h := &HttpMux{server}

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

func (h *HttpMux) Stop() error {
	// TODO(olegs): shut down the agents gracefully
	return nil
}
