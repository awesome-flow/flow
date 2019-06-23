package app

import (
	"fmt"
	"net/http"

	core "github.com/awesome-flow/flow/pkg/corev1alpha1"
	"github.com/awesome-flow/flow/pkg/types"
	"github.com/awesome-flow/flow/web/app/agent"
)

type HttpMux struct {
	server *http.Server
}

func newAdminSrvMx(ctx *core.Context) (*http.ServeMux, error) {
	srvMx := http.NewServeMux()

	for _, ar := range agent.AllAgentRegistrators() {
		wa, err := ar(ctx)
		if err != nil {
			return nil, err
		}
		srvMx.Handle(wa.GetPath(), wa.GetHandler())
	}

	return srvMx, nil
}

func NewHttpMux(ctx *core.Context) (*HttpMux, error) {
	syscfg, ok := ctx.Config().Get(types.NewKey("system"))
	if !ok {
		return nil, fmt.Errorf("failed to get system config from the pipeline context")
	}
	srvMx, err := newAdminSrvMx(ctx)
	if err != nil {
		return nil, err
	}
	server := &http.Server{
		Addr:    syscfg.(types.CfgBlockSystem).Admin.Bind,
		Handler: srvMx,
	}
	h := &HttpMux{server}

	go func() {
		if err := server.ListenAndServe(); err != nil {
			switch err {
			case http.ErrServerClosed:
				ctx.Logger().Info("Admin server closed")
			default:
				ctx.Logger().Fatal("Admin server critical error: %s", err)
			}
		}
	}()

	return h, nil
}

func (h *HttpMux) Stop() error {
	// TODO(olegs): shut down the agents gracefully
	return nil
}
