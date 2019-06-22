package agent

import (
	"net/http"

	"github.com/awesome-flow/flow/pkg/corev1alpha1/pipeline"
)

type WebAgent interface {
	GetPath() string
	GetHandler() http.Handler
}

type WebAgents []WebAgent

func (w WebAgents) Len() int           { return len(w) }
func (w WebAgents) Swap(i, j int)      { w[i], w[j] = w[j], w[i] }
func (w WebAgents) Less(i, j int) bool { return w[i].GetPath() < w[j].GetPath() }

type DummyWebAgent struct {
	path    string
	handler http.Handler
}

func NewDummyWebAgent(path string, handler http.HandlerFunc) *DummyWebAgent {
	return &DummyWebAgent{
		path:    path,
		handler: handler,
	}
}

func (dwa *DummyWebAgent) GetPath() string {
	return dwa.path
}

func (dwa *DummyWebAgent) GetHandler() http.Handler {
	return dwa.handler
}

type WebAgentRegistrator func(*pipeline.Pipeline) (WebAgent, error)
type WebAgentRegistrators []WebAgentRegistrator

var (
	webAgentRegistrators = make(WebAgentRegistrators, 0)
)

func RegisterWebAgent(r WebAgentRegistrator) {
	webAgentRegistrators = append(webAgentRegistrators, r)
}

func AllAgentRegistrators() WebAgentRegistrators {
	return webAgentRegistrators
}
