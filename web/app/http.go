package app

import (
	"fmt"
	"net/http"

	core "github.com/awesome-flow/flow/pkg/corev1alpha1"
	"github.com/awesome-flow/flow/pkg/types"
	"github.com/awesome-flow/flow/web/app/agent"
)

type HttpMux struct {
	ctx    *core.Context
	server *http.Server
	agents agent.WebAgents
	done   chan struct{}
}

var _ core.Runner = (*HttpMux)(nil)

func NewHttpMux(ctx *core.Context) (*HttpMux, error) {
	syscfg, ok := ctx.Config().Get(types.NewKey("system"))
	if !ok {
		return nil, fmt.Errorf("failed to get system config from the pipeline context")
	}

	srvMx := http.NewServeMux()
	regs := agent.AllAgentRegistrators()
	agents := make(agent.WebAgents, 0, len(regs))

	for _, ar := range regs {
		wa, err := ar(ctx)
		if err != nil {
			return nil, err
		}
		srvMx.Handle(wa.GetPath(), wa.GetHandler())
		agents = append(agents, wa)
	}

	server := &http.Server{
		Addr:    syscfg.(types.CfgBlockSystem).Admin.Bind,
		Handler: srvMx,
	}

	return &HttpMux{
		ctx:    ctx,
		server: server,
		agents: agents,
		done:   make(chan struct{}),
	}, nil
}

func (h *HttpMux) Start() error {
	for _, wa := range h.agents {
		if err := wa.Start(); err != nil {
			return err
		}
	}
	go func() {
		if err := h.server.ListenAndServe(); err != nil {
			switch err {
			case http.ErrServerClosed:
				h.ctx.Logger().Info("admin server closed")
			default:
				h.ctx.Logger().Fatal("admin server critical error: %s", err)
			}
			close(h.done)
		}
	}()

	return nil
}

func (h *HttpMux) Stop() error {
	for _, wa := range h.agents {
		if err := wa.Stop(); err != nil {
			return err
		}
	}

	err := h.server.Close()
	<-h.done

	return err
}
