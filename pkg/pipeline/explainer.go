package pipeline

import (
	"fmt"
	"strings"
	"text/template"
)

type Explainer interface {
	Explain(*Pipeline) string
}

type DotExplainer struct{}

const PipelineDotTmpl = `digraph G{
{{range $compname, $compcfg := . -}}
  {{- if $compcfg.IsDisconnected -}}
    {{$compname}}
  {{- end -}}
  {{- if $compcfg.Connect -}}
    {{$compname}} -> {{$compcfg.Connect}}
  {{- end -}}
  {{- if $compcfg.Links -}}
    {{- range $compcfg.Links}}
      {{if $compcfg.Connect -}}
        {{.}} -> {{$compname}}
      {{- else -}}
        {{$compname}} -> {{.}}
      {{- end -}}
    {{- end -}}
  {{- end -}}
  {{- if $compcfg.Routes -}}
    {{range $key, $route := $compcfg.Routes -}}
      {{$compname}} -> {{$route}} [label="{{$key}}"]
    {{end}}
  {{- end}}
{{end -}}
}`

func (de *DotExplainer) Explain(pipeline *Pipeline) (string, error) {
	tmpl, err := template.New("pipeline-dot").Parse(PipelineDotTmpl)
	if err != nil {
		return "", fmt.Errorf("Failed to parse template: %s", err.Error())
	}
	builder := &strings.Builder{}
	if err := tmpl.Execute(builder, pipeline.pplCfg); err != nil {
		return "", fmt.Errorf("Failed to render data: %s", err.Error())
	}

	return builder.String(), nil
}
