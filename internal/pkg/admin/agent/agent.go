package agent

import "net/http"

type webHandler func(rw http.ResponseWriter, req *http.Request)

type WebAgent interface {
	GetPath() string
	GetHandler() webHandler
}

type DummyWebAgent struct {
	path    string
	handler webHandler
}

func (dwa *DummyWebAgent) GetPath() string {
	return dwa.path
}

func (dwa *DummyWebAgent) GetHandler() webHandler {
	return dwa.handler
}

func NewDummyWebAgent(path string, handler webHandler) *DummyWebAgent {
	return &DummyWebAgent{
		path:    path,
		handler: handler,
	}
}

var (
	webAgents = make([]WebAgent, 0)
)

func RegisterWebAgent(a WebAgent) {
	webAgents = append(webAgents, a)
}

func AllAgents() []WebAgent {
	return webAgents
}
