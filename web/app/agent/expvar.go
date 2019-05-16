package agent

import (
	"expvar"
	"net/http"
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
	RegisterWebAgent(NewExpvarWebAgent("/expvar"))
}
