package main

import (
	"flag"
	"log"
	"time"

	remote_file "github.com/whiteboxio/flow/pkg/util/file/remote_http_file"
)

func main() {
	cfgPath := flag.String("cfg-path", "", "Config HTTP address")
	flag.Parse()

	rhf, err := remote_file.NewWithInterval(*cfgPath, time.Second)
	if err != nil {
		log.Fatalf("Failed to instantiate a new remote file: %s", err)
	}

	if err := rhf.Deploy(); err != nil {
		log.Fatalf("Failed to deploy remote file watcher: %s", err)
	}
	data, err := rhf.ReadData()
	if err != nil {
		log.Fatalf("Failed to read remote data: %s", err)
	}
	log.Printf("Remote data: %s", data)

	for upd := range rhf.GetNotifyChan() {
		log.Printf("Received a new notification: %+v", upd)
		data, err := rhf.ReadData()
		if err != nil {
			log.Printf("Error re-reading the data: %s", err)
		} else {
			log.Printf("Updated data: %s", data)
		}
	}
}
