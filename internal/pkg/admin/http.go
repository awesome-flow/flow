package admin

import (
	"fmt"
	"net/http"
	"sort"
	"strings"

	log "github.com/sirupsen/logrus"
	"github.com/whiteboxio/flow/pkg/config"
	"github.com/whiteboxio/flow/pkg/metrics"
)

type HTTP struct {
	server *http.Server
}

func newAdminSrvMx(cfg *config.CfgBlockSystem) *http.ServeMux {
	srvMx := http.NewServeMux()

	srvMx.HandleFunc("/config", func(rw http.ResponseWriter, req *http.Request) {
		cfg := config.GetAll()
		respChunks := make([]string, 0)
		for k, v := range cfg {
			respChunks = append(respChunks, fmt.Sprintf("%s: %s", k, v))
		}
		sort.Strings(respChunks)
		rw.WriteHeader(http.StatusOK)
		rw.Write([]byte(strings.Join(respChunks, "\n")))
	})

	srvMx.HandleFunc("/metrics", func(rw http.ResponseWriter, req *http.Request) {
		mtrx := metrics.GetAll()
		log.Infof("Metrics: %+v", mtrx)
		respChunks := make([]string, 0)
		for k, v := range mtrx {
			respChunks = append(respChunks, fmt.Sprintf("%s: %d", k, v))
		}
		sort.Strings(respChunks)
		rw.WriteHeader(http.StatusOK)
		rw.Write([]byte(strings.Join(respChunks, "\n")))
	})

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
				panic(fmt.Sprintf("Admin server critical error: %s", err))
			}
		}
	}()

	return h, nil
}
