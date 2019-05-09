package agent

import (
	"fmt"
	"net/http"
	"sort"
)

func init() {
	RegisterWebAgent(
		NewDummyWebAgent(
			"/",
			func(rw http.ResponseWriter, req *http.Request) {
				sort.Sort(webAgents)
				rw.WriteHeader(http.StatusOK)
				rw.Write([]byte("<html><body><h1>flowd admin interface</h1><ul>"))
				var path string
				for _, agent := range webAgents {
					path = agent.GetPath()
					if path == "/" {
						// do not register ourselves
						continue
					}
					rw.Write([]byte(fmt.Sprintf("<li><a href='%s'>%[1]s</a></li>",
						path)))
				}
				rw.Write([]byte("</ul></body></html>"))
			},
		),
	)
}
