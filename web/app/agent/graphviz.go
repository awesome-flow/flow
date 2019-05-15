package agent

import (
	"net/http"

	log "github.com/sirupsen/logrus"

	"github.com/awesome-flow/flow/pkg/global"
	"github.com/awesome-flow/flow/pkg/pipeline"
)

type DescribePage struct {
	Title    string
	GraphViz string
}

func init() {
	RegisterWebAgent(
		NewDummyWebAgent(
			"/pipeline/describe",
			func(rw http.ResponseWriter, req *http.Request) {
				pipelineitf, ok := global.Load("pipeline")
				if !ok {
					log.Errorf("Failed to fetch pipeline from the global registry")
					rw.WriteHeader(http.StatusInternalServerError)
					return
				}
				pipeline, ok := pipelineitf.(*pipeline.Pipeline)
				if !ok {
					log.Errorf("Failed to cast pipeline to the propper data type. Probably data corruption")
					rw.WriteHeader(http.StatusInternalServerError)
					return
				}

				expl, err := pipeline.Explain()
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
		),
	)
}
