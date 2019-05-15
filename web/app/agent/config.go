package agent

import (
	"net/http"

	"github.com/awesome-flow/flow/pkg/cfg"
	"github.com/awesome-flow/flow/pkg/global"
)

func init() {
	RegisterWebAgent(
		NewDummyWebAgent(
			"/config",
			func(rw http.ResponseWriter, req *http.Request) {
				repo, ok := global.Load("config")
				if !ok {
					rw.WriteHeader(http.StatusInternalServerError)
					rw.Write([]byte("Failed to fetch config repo"))
					return
				}
				cfgdata := repo.(*cfg.Repository).Explain()
				respondWith(rw, RespJson, "", cfgdata)
			},
		),
	)
}
