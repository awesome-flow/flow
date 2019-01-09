package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"

	volatile_file "github.com/awesome-flow/flow/pkg/util/file/volatile_file"
)

type VolatileJsonFile struct {
	*volatile_file.VolatileFile
}

func NewVJF(path string) (*VolatileJsonFile, error) {
	vf, err := volatile_file.New(path)
	if err != nil {
		return nil, err
	}
	return &VolatileJsonFile{vf}, nil
}

type Timestamp struct {
	Time uint64
}

func (vjf *VolatileJsonFile) ReadData() (interface{}, error) {
	rawData, err := vjf.ReadRawData()
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
	path := flag.String("path", "", "Watchable local file path")
	flag.Parse()
	vjf, err := NewVJF(*path)
	if err != nil {
		log.Fatalf("Failed to instantiate a new volatile file: %s", err)
	}
	if err := vjf.Deploy(); err != nil {
		log.Fatalf("Failed to deploy the volatile file: %s", err)
	}
	data, err := vjf.ReadData()
	if err != nil {
		log.Fatalf("Failed to read the data: %s", err)
	}
	log.Printf("First-time red data: %+v", data)
	for ntf := range vjf.GetNotifyChan() {
		log.Printf("Received an upd notification: %s", ntf)
		data, err := vjf.ReadData()
		if err != nil {
			fmt.Errorf("Failed to read the data in the loop: %s", err)
		}
		fmt.Printf("Re-red data: %+v", data)
	}
}
