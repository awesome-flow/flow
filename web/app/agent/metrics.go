package agent

import (
	"fmt"
	"net/http"
	"sort"
	"strings"

	"github.com/awesome-flow/flow/pkg/corev1alpha1/pipeline"
	"github.com/awesome-flow/flow/pkg/metrics"
)

func init() {
	RegisterWebAgent(
		func(*pipeline.Pipeline) (WebAgent, error) {
			return NewDummyWebAgent(
				"/metrics",
				func(rw http.ResponseWriter, req *http.Request) {
					mtrx := metrics.GetAllMetrics()
					respChunks := make([]string, 0)
					for k, metric := range mtrx {
						switch metric := metric.(type) {

						case *metrics.Counter:
							respChunks = append(respChunks, fmt.Sprintf("%s: %d", k, metric.Get()))
						case *metrics.Gauge: //Same as counter
							respChunks = append(respChunks, fmt.Sprintf("%s: %d", k, metric.Get()))
						default:
						}

					}
					sort.Strings(respChunks)
					rw.WriteHeader(http.StatusOK)
					rw.Write([]byte(strings.Join(respChunks, "\n")))
				},
			), nil
		},
	)
}
