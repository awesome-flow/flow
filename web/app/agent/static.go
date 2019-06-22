package agent

import (
	"net/http"

	"github.com/awesome-flow/flow/pkg/corev1alpha1/pipeline"
)

func init() {
	RegisterWebAgent(
		func(*pipeline.Pipeline) (WebAgent, error) {
			return &DummyWebAgent{
				"/static/",
				http.StripPrefix("/static/", http.FileServer(http.Dir("./web/static"))),
			}, nil
		},
	)
	RegisterWebAgent(
		func(*pipeline.Pipeline) (WebAgent, error) {
			return &DummyWebAgent{
				"/favicon.ico",
				http.FileServer(http.Dir("./web/static/img")),
			}, nil
		},
	)
}
