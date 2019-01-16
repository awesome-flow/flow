package agent

import (
	"fmt"
	"net/http"
	"sort"
	"strings"

	"github.com/awesome-flow/flow/pkg/metrics"
)

func init() {
	RegisterWebAgent(
		NewDummyWebAgent(
			"/metrics",
			func(rw http.ResponseWriter, req *http.Request) {
				mtrx := metrics.GetAllCounters()
				respChunks := make([]string, 0)
				for k, v := range mtrx {
					respChunks = append(respChunks, fmt.Sprintf("%s: %d", k, v))
				}
				sort.Strings(respChunks)
				rw.WriteHeader(http.StatusOK)
				rw.Write([]byte(strings.Join(respChunks, "\n")))
			},
		),
	)
}
