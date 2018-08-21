package agent

import (
	"fmt"
	"net/http"
	"sort"
	"strings"

	"github.com/whiteboxio/flow/pkg/config"
)

func init() {
	RegisterWebAgent(
		NewDummyWebAgent(
			"/config",
			func(rw http.ResponseWriter, req *http.Request) {
				cfg := config.GetAll()
				respChunks := make([]string, 0)
				for k, vItf := range cfg {
					if v, convOk := vItf.(string); convOk {
						respChunks = append(respChunks, fmt.Sprintf("%s: %s", k, v))
					} else if v, convOk := vItf.(*string); convOk {
						respChunks = append(respChunks, fmt.Sprintf("%s: %s", k, *v))
					} else {
						respChunks = append(respChunks, fmt.Sprintf("%s: %+v", k, vItf))
					}
				}
				sort.Strings(respChunks)
				rw.WriteHeader(http.StatusOK)
				rw.Write([]byte(strings.Join(respChunks, "\n")))
			},
		),
	)
}
