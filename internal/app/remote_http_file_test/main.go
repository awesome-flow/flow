package main

import (
	"encoding/json"
	"flag"
	"log"
	"time"

	remote_file "github.com/whiteboxio/flow/pkg/util/file/remote_http_file"
)

type Timestamp struct {
	Time int
}

type RemoteJsonFile struct {
	*remote_file.RemoteHttpFile
}

func NewRJF(path string, interval time.Duration) (*RemoteJsonFile, error) {
	rhf, err := remote_file.NewWithInterval(path, interval)
	if err != nil {
		return nil, err
	}
	rjf := &RemoteJsonFile{
		rhf,
	}
	return rjf, nil
}

func (rjf *RemoteJsonFile) ReadData() (interface{}, error) {
	rawData, err := rjf.ReadRawData()
	if err != nil {
		return nil, err
	}
	v := &Timestamp{}
	if err := json.Unmarshal(rawData, v); err != nil {
		return nil, err
	}
	return v, nil
}

func main() {
	cfgPath := flag.String("cfg-path", "", "Config HTTP address")
	flag.Parse()

	rjf, err := NewRJF(*cfgPath, time.Second)
	if err != nil {
		log.Fatalf("Failed to instantiate a new remote file: %s", err)
	}

	if err := rjf.Deploy(); err != nil {
		log.Fatalf("Failed to deploy remote file watcher: %s", err)
	}
	data, err := rjf.ReadData()
	if err != nil {
		log.Fatalf("Failed to read remote data: %s", err)
	}
	log.Printf("Remote data: %+v", data)

	for upd := range rjf.GetNotifyChan() {
		log.Printf("Received a new notification: %+v", upd)
		data, err := rjf.ReadData()
		if err != nil {
			log.Printf("Error re-reading the data: %s", err)
		} else {
			log.Printf("Updated data: %+v", data)
		}
	}
}
