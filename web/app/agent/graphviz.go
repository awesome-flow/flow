package agent

import (
	"net/http"

	"github.com/awesome-flow/flow/pkg/corev1alpha1/pipeline"
	log "github.com/sirupsen/logrus"
)

type DescribePage struct {
	Title    string
	GraphViz string
}

func init() {
	RegisterWebAgent(
		func(ppl *pipeline.Pipeline) (WebAgent, error) {
			return NewDummyWebAgent(
				"/pipeline/describe",
				func(rw http.ResponseWriter, req *http.Request) {
					expl, err := ppl.Explain()
					if err != nil {
						log.Errorf("Failed to explain the pipeline: %s", err.Error())
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
