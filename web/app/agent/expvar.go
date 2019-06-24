package agent

import (
	"expvar"
	"net/http"

	core "github.com/awesome-flow/flow/pkg/corev1alpha1"
)

type ExpvarWebAgent struct {
	path string
}

func NewExpvarWebAgent(path string) *ExpvarWebAgent {
	return &ExpvarWebAgent{path: path}
}

func (eva *ExpvarWebAgent) GetPath() string {
	return eva.path
}

func (eva *ExpvarWebAgent) GetHandler() http.Handler {
	return expvar.Handler()
}

func (eva *ExpvarWebAgent) Start() error {
	return nil
}

func (eva *ExpvarWebAgent) Stop() error {
	return nil
}

func init() {
	RegisterWebAgent(
		func(ctx *core.Context) (WebAgent, error) {
			return NewExpvarWebAgent("/expvar"), nil
		},
	)
}
