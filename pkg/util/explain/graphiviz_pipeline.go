package explain

import (
	"bytes"
	"fmt"
	"reflect"
	"text/template"

	"github.com/awesome-flow/flow/pkg/types"
)

const PipelineDotTmpl = `digraph Flow{
{{range $aname, $acfg := . -}}
  {{- if $acfg.Connect -}}
  {{- range $ix, $c := $acfg.Connect}}
    "{{$aname}}" -> "{{$c}}"
  {{end}}
  {{- else -}}
    "{{$aname}}"
  {{end}}
{{end -}}
}`

type Pipeline struct{}

var _ Explainer = (*Pipeline)(nil)

func (p *Pipeline) Explain(in interface{}) ([]byte, error) {
	cfg, ok := in.(map[string]types.CfgBlockPipeline)
	if !ok {
		return nil, fmt.Errorf("unexpected input type: %s", reflect.TypeOf(in).Name())
	}
	tmpl, err := template.New("cfgpipeline-dot").Parse(PipelineDotTmpl)
	if err != nil {
		return nil, fmt.Errorf("failed to parse template: %s", err.Error())
	}
	buf := new(bytes.Buffer)
	if err := tmpl.Execute(buf, cfg); err != nil {
		return nil, fmt.Errorf("failed to render data: %s", err.Error())
	}

	return buf.Bytes(), nil
}
