package explain

import (
	"fmt"
	"strings"
	"text/template"

	"github.com/awesome-flow/flow/pkg/types"
)

const PipelineDotTmpl = `digraph Flow{
{{range $aname, $acfg := . -}}
  {{- if $acfg.Connect -}}
  {{- range $ix, $c := $acfg.Connect}}
    {{$aname}} -> {{$c}}
  {{end}}
  {{- else -}}
    {{$aname}}
  {{end}}
{{end -}}
}`

func ExplainCfgPpl(cfgppl map[string]types.CfgBlockPipeline) (string, error) {
	tmpl, err := template.New("cfgpipeline-dot").Parse(PipelineDotTmpl)
	if err != nil {
		return "", fmt.Errorf("Failed to parse template: %s", err.Error())
	}
	builder := &strings.Builder{}
	if err := tmpl.Execute(builder, cfgppl); err != nil {
		return "", fmt.Errorf("Failed to render data: %s", err.Error())
	}

	return builder.String(), nil
}
