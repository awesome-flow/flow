package agent

import (
	"html/template"
	"net/http"

	log "github.com/sirupsen/logrus"

	"github.com/awesome-flow/flow/pkg/global"
	"github.com/awesome-flow/flow/pkg/pipeline"
)

const GraphVizTemplate = `
<textarea style="display:none;" name="graphviz-dot-text" id="graphviz-dot-text">
	{{.GraphViz}}
</textarea>
<div id="graph-place"></div>
<script src="http://www.webgraphviz.com/viz.js"></script>
<script type='text/javascript'>
	document.addEventListener("DOMContentLoaded", function(event) {
		var dotsrc = document.getElementById("graphviz-dot-text")
		var data = Viz(dotsrc.value, "svg")
		var placeholder = document.getElementById("graph-place")
		placeholder.innerHTML = data
	})
</script>
`

type GraphVizAgent struct {
	tmpl *template.Template
}

var graphvizagent *GraphVizAgent

func NewGraphVizAgent() (*GraphVizAgent, error) {
	tmpl, err := template.New("graphviz").Parse(GraphVizTemplate)
	if err != nil {
		return nil, err
	}
	return &GraphVizAgent{tmpl}, nil
}

const graphvizmock = `
digraph G {
  "Welcome" -> "To"
  "To" -> "Web"
  "To" -> "GraphViz!"
}
`

func (ga *GraphVizAgent) renderGraphViz(rw http.ResponseWriter, req *http.Request) {

	pipelineitf, ok := global.Get("pipeline")
	if !ok {
		log.Errorf("Failed to fetch pipeline from the global registry")
		rw.WriteHeader(http.StatusInternalServerError)
		return
	}
	pipeline, ok := pipelineitf.(*pipeline.Pipeline)
	if !ok {
		log.Errorf("Failed to cast pipeline to the propper data type. Probably data corruption")
		rw.WriteHeader(http.StatusInternalServerError)
		return
	}

	explanation, err := pipeline.Explain()
	if err != nil {
		log.Errorf("Failed to explain the pipeline: %s", err.Error())
		rw.WriteHeader(http.StatusInternalServerError)
		return
	}

	data := struct {
		Title    string
		GraphViz string
	}{
		Title:    "Flowd pipeline render",
		GraphViz: explanation,
	}
	rw.Header().Set("Content-Type", "text/html")
	ga.tmpl.Execute(rw, data)
}

func init() {
	var err error
	graphvizagent, err = NewGraphVizAgent()
	if err != nil {
		panic(err.Error())
	}
	RegisterWebAgent(
		NewDummyWebAgent(
			"/graphviz",
			graphvizagent.renderGraphViz,
		),
	)
}
