package agent

import (
	"net/http"
	"net/http/pprof"

	core "github.com/awesome-flow/flow/pkg/corev1alpha1"
)

type PprofPage struct {
	Title string
}

func init() {
	RegisterWebAgent(
		func(*core.Context) (WebAgent, error) {
			return NewDummyWebAgent(
				"/pprof/",
				//pprof.Index,
				func(rw http.ResponseWriter, req *http.Request) {
					respondWith(rw, RespHtml, "pprof", &PprofPage{Title: "pprof debug info"})
				},
			), nil
		},
	)

	RegisterWebAgent(
		func(*core.Context) (WebAgent, error) {
			return NewDummyWebAgent(
				"/pprof/cmdline",
				pprof.Cmdline,
			), nil
		},
	)

	RegisterWebAgent(
		func(*core.Context) (WebAgent, error) {
			return NewDummyWebAgent(
				"/pprof/profile",
				pprof.Profile,
			), nil
		},
	)

	RegisterWebAgent(
		func(*core.Context) (WebAgent, error) {
			return NewDummyWebAgent(
				"/pprof/symbol",
				pprof.Symbol,
			), nil
		},
	)

	RegisterWebAgent(
		func(*core.Context) (WebAgent, error) {
			return NewDummyWebAgent(
				"/pprof/trace",
				pprof.Trace,
			), nil
		},
	)
}
