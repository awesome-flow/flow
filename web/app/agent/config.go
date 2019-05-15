package agent

import (
	"encoding/json"
	"net/http"

	"github.com/awesome-flow/flow/pkg/cfg"
	"github.com/awesome-flow/flow/pkg/global"
)

type ConfigPage struct {
	Title  string
	Config string
}

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
				js, err := json.Marshal(cfgdata)
				if err != nil {
					rw.WriteHeader(http.StatusInternalServerError)
					rw.Write([]byte(err.Error()))
					return
				}
				respondWith(rw, RespHtml, "config", &ConfigPage{
					Title:  "Flow active config",
					Config: string(js),
				})
			},
		),
	)
}
