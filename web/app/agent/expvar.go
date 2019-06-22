package agent

import (
	"expvar"
	"net/http"

	"github.com/awesome-flow/flow/pkg/corev1alpha1/pipeline"
)

type ExpvarWebAgent struct {
	path string
}

func (eva *ExpvarWebAgent) GetPath() string {
	return eva.path
}

func (eva *ExpvarWebAgent) GetHandler() http.Handler {
	return expvar.Handler()
}

func NewExpvarWebAgent(path string) *ExpvarWebAgent {
	return &ExpvarWebAgent{path: path}
}

func init() {
	RegisterWebAgent(
		func(*pipeline.Pipeline) (WebAgent, error) {
			return NewExpvarWebAgent("/expvar"), nil
		},
	)
}
