package agent

import (
	"bytes"
	"encoding/json"
	"fmt"
	"html/template"
	"net/http"
)

var (
	tmpl *template.Template
)

func init() {
	var err error
	tmpl, err = template.ParseFiles(
		"web/template/page.gohtml",
		"web/template/page/index.gohtml",
		"web/template/page/graphviz.gohtml",
	)
	if err != nil {
		panic(err.Error())
	}
}

type Page struct {
	Data interface{}
	Body template.HTML
}

type ResponseFormat uint8

const (
	RespHtml ResponseFormat = iota
	RespJson
)

const (
	HdrContentType  = "Content-Type"
	ContentTypeHtml = "text/html"
	ContentTypeJson = "application/json"
)

func respondWith(rw http.ResponseWriter, format ResponseFormat, tmplName string, data interface{}) error {
	var err error
	switch format {
	case RespHtml:
		err = respondWithHtml(rw, tmplName, data)
	case RespJson:
		err = respondWithJson(rw, data)
	default:
		err = fmt.Errorf("Unknown response format: %d", format)
	}
	if err != nil {
		rw.WriteHeader(http.StatusInternalServerError)
		rw.Write([]byte(fmt.Sprintf("Internal Server Error: %s", err)))
		return err
	}
	return nil
}

func respondWithHtml(rw http.ResponseWriter, tmplName string, data interface{}) error {
	rw.Header().Add(HdrContentType, ContentTypeHtml)
	bw := bytes.NewBuffer(nil)
	if err := tmpl.ExecuteTemplate(bw, tmplName, data); err != nil {
		return err
	}
	if err := tmpl.ExecuteTemplate(rw, "layout", &Page{Data: data, Body: template.HTML(bw.Bytes())}); err != nil {
		return err
	}
	return nil
}

func respondWithJson(rw http.ResponseWriter, data interface{}) error {
	js, err := json.Marshal(data)
	if err != nil {
		return err
	}
	rw.Header().Add(HdrContentType, ContentTypeJson)
	if _, err := rw.Write(js); err != nil {
		return err
	}
	return nil
}

type IndexPage struct {
	Title string
}

func init() {
	RegisterWebAgent(
		NewDummyWebAgent(
			"/",
			func(rw http.ResponseWriter, req *http.Request) {
				respondWith(rw, RespHtml, "index", &IndexPage{Title: "Flow admin interface"})
			},
		),
	)
}
