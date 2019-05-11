package agent

import (
	"html/template"
	"net/http"
	"sort"
)

var (
	TmplPage *template.Template
)

func init() {
	var err error
	TmplPage, err = template.ParseFiles("web/template/page.gohtml")
	if err != nil {
		panic(err.Error())
	}
}

type Page struct {
	Title string
	Body  string
}

func init() {
	RegisterWebAgent(
		NewDummyWebAgent(
			"/",
			func(rw http.ResponseWriter, req *http.Request) {
				sort.Sort(webAgents)
				page := &Page{
					Title: "Flow admin interface",
					Body:  "Hello Flow!",
				}
				TmplPage.Execute(rw, page)
				// rw.WriteHeader(http.StatusOK)

				// rw.Write([]byte("<html><body><h1>flowd admin interface</h1><ul>"))
				// var path string
				// for _, agent := range webAgents {
				// 	path = agent.GetPath()
				// 	if path == "/" {
				// 		// do not register ourselves
				// 		continue
				// 	}
				// 	rw.Write([]byte(fmt.Sprintf("<li><a href='%s'>%[1]s</a></li>",
				// 		path)))
				// }
				// rw.Write([]byte("</ul></body></html>"))
			},
		),
	)
}
