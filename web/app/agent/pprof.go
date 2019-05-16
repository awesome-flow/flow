package agent

import (
	"net/http"
	"net/http/pprof"
)

type PprofPage struct {
	Title string
}

func init() {
	RegisterWebAgent(
		NewDummyWebAgent(
			"/pprof/",
			//pprof.Index,
			func(rw http.ResponseWriter, req *http.Request) {
				respondWith(rw, RespHtml, "pprof", &PprofPage{Title: "pprof debug info"})
			},
		),
	)

	RegisterWebAgent(
		NewDummyWebAgent(
			"/pprof/cmdline",
			pprof.Cmdline,
		),
	)

	RegisterWebAgent(
		NewDummyWebAgent(
			"/pprof/profile",
			pprof.Profile,
		),
	)

	RegisterWebAgent(
		NewDummyWebAgent(
			"/pprof/symbol",
			pprof.Symbol,
		),
	)

	RegisterWebAgent(
		NewDummyWebAgent(
			"/pprof/trace",
			pprof.Trace,
		),
	)
}
