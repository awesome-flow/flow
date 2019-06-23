package agent

import (
	"net/http"

	core "github.com/awesome-flow/flow/pkg/corev1alpha1"
)

type DescribePage struct {
	Title    string
	GraphViz string
}

func init() {
	RegisterWebAgent(
		func(ctx *core.Context) (WebAgent, error) {
			return NewDummyWebAgent(
				"/pipeline/describe",
				func(rw http.ResponseWriter, req *http.Request) {
					// TODO: expl, err := ppl.Explain()
					var err error
					expl := "digraph D { A -> B }"
					if err != nil {
						ctx.Logger().Error("Failed to explain the pipeline: %s", err)
						rw.WriteHeader(http.StatusInternalServerError)
						return
					}
					respondWith(rw, RespHtml, "graphviz", &DescribePage{
						Title:    "Flow Pipeline",
						GraphViz: expl,
					})
				},
			), nil
		},
	)
}
