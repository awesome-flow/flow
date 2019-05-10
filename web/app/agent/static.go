package agent

import (
	"net/http"
)

func init() {
	RegisterWebAgent(
		&DummyWebAgent{
			"/static/",
			http.StripPrefix("/static/", http.FileServer(http.Dir("./web/static"))),
		},
	)
}
