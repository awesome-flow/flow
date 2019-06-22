package app

import (
	"fmt"
	"net/http"

	"github.com/awesome-flow/flow/pkg/corev1alpha1/pipeline"
	"github.com/awesome-flow/flow/pkg/types"
	"github.com/awesome-flow/flow/web/app/agent"
	log "github.com/sirupsen/logrus"
)

type HttpMux struct {
	server *http.Server
}

func newAdminSrvMx(ppl *pipeline.Pipeline) (*http.ServeMux, error) {
	srvMx := http.NewServeMux()

	for _, ar := range agent.AllAgentRegistrators() {
		wa, err := ar(ppl)
		if err != nil {
			return nil, err
		}
		srvMx.Handle(wa.GetPath(), wa.GetHandler())
	}

	return srvMx, nil
}

func NewHttpMux(ppl *pipeline.Pipeline) (*HttpMux, error) {
	cfg, ok := ppl.Context().Config().Get(types.NewKey("system"))
	if !ok {
		return nil, fmt.Errorf("failed to get system config from the pipeline context")
	}
	srvMx, err := newAdminSrvMx(ppl)
	if err != nil {
		return nil, err
	}
	server := &http.Server{
		Addr:    cfg.(types.CfgBlockSystem).Admin.Bind,
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
