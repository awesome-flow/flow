package agent

import (
	"net/http"

	core "github.com/awesome-flow/flow/pkg/corev1alpha1"
)

func init() {
	RegisterWebAgent(
		func(*core.Context) (WebAgent, error) {
			return &DummyWebAgent{
				"/static/",
				http.StripPrefix("/static/", http.FileServer(http.Dir("./web/static"))),
			}, nil
		},
	)
	RegisterWebAgent(
		func(*core.Context) (WebAgent, error) {
			return &DummyWebAgent{
				"/favicon.ico",
				http.FileServer(http.Dir("./web/static/img")),
			}, nil
		},
	)
}
