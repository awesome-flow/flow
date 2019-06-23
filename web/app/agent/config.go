package agent

import (
	"encoding/json"
	"net/http"

	core "github.com/awesome-flow/flow/pkg/corev1alpha1"
)

type ConfigPage struct {
	Title  string
	Config string
}

func init() {
	RegisterWebAgent(
		func(ctx *core.Context) (WebAgent, error) {
			return NewDummyWebAgent(
				"/config",
				func(rw http.ResponseWriter, req *http.Request) {
					cfgdata := ctx.Config().Explain()
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
			), nil
		},
	)
}
