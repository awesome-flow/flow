package agent

import (
	"net/http/pprof"
)

func init() {
	RegisterWebAgent(
		NewDummyWebAgent(
			"/pprof/",
			pprof.Index,
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
