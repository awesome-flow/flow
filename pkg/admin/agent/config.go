package agent

import (
	"fmt"
	"net/http"
	"sort"
	"strings"

	"github.com/awesome-flow/flow/pkg/config"
)

func init() {
	RegisterWebAgent(
		NewDummyWebAgent(
			"/config",
			func(rw http.ResponseWriter, req *http.Request) {
				cfg := config.GetAll()
				respChunks := make([]string, 0)
				for k, vItf := range cfg {
					switch vItf.(type) {
					case string:
						respChunks = append(respChunks, fmt.Sprintf("%s: %s", k, vItf))
					case *string:
						respChunks = append(respChunks, fmt.Sprintf("%s: %s", k, *(vItf.(*string))))
					default:
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
