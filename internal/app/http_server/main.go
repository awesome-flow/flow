package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"path"
	"strings"
)

type sourceList []string

func (v *sourceList) String() string {
	return strings.Join(*v, ", ")
}

func (v *sourceList) Set(strVal string) error {
	*v = append(*v, strVal)
	return nil
}

var sources sourceList

func main() {
	addr := flag.String("addr", ":8080", "Web server bind address")
	flag.Var(&sources, "source", "The source to serve as response (might be plural)")

	flag.Parse()

	if len(sources) == 0 {
		log.Fatalf("Nothing to serve (no values provided wuth -source). Exiting.")
	}

	srcNames := make(map[string]string)
	for _, src := range sources {
		srcNames[path.Base(src)] = src
	}

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Header().Set("Content-Type", "text/html")
		strBody := "<html><h4>Servable files:</h4><ul>"
		for srcName := range srcNames {
			strBody += fmt.Sprintf("<li><a href=\"/%s\">%s</a></li>", srcName, srcName)
		}
		strBody += "</ul></html>"
		w.Write([]byte(strBody))
	})

	for k := range srcNames {
		func(srcName string) {
			http.HandleFunc("/"+srcName, func(w http.ResponseWriter, r *http.Request) {
				log.Printf(r.URL.String())
				data, err := ioutil.ReadFile(srcNames[srcName])
				if err != nil {
					w.WriteHeader(http.StatusInternalServerError)
					w.Write([]byte(err.Error()))
				}
				w.WriteHeader(http.StatusOK)
				w.Header().Set("Content-Type", "application/json")
				w.Write(data)
			})
		}(k)
	}

	log.Printf("The server is ready")

	log.Fatal(http.ListenAndServe(*addr, nil))
}
