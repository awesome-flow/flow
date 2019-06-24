package agent

import (
	"fmt"
	"net/http"

	core "github.com/awesome-flow/flow/pkg/corev1alpha1"
	"github.com/awesome-flow/flow/pkg/types"
	explain "github.com/awesome-flow/flow/pkg/util/explain"
)

type DescribePage struct {
	Title    string
	GraphViz string
}

func init() {
	RegisterWebAgent(
		func(ctx *core.Context) (WebAgent, error) {
			cfgppl, ok := ctx.Config().Get(types.NewKey("pipeline"))
			if !ok {
				return nil, fmt.Errorf("failed to get `pipeline` config")
			}
			e := new(explain.Pipeline)
			expl, err := e.Explain(cfgppl)
			if err != nil {
				return nil, err
			}

			return NewDummyWebAgent(
				"/pipeline/describe",
				func(rw http.ResponseWriter, req *http.Request) {
					respondWith(rw, RespHtml, "graphviz", &DescribePage{
						Title:    "Flow Pipeline",
						GraphViz: string(expl),
					})
				},
			), nil
		},
	)
}
