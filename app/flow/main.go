package main

import (
	"fmt"
	"os"
	"os/signal"

	log "github.com/sirupsen/logrus"
	"github.com/whiteboxio/flow/pkg/config"
	"github.com/whiteboxio/flow/pkg/pipeline"
)

const (
	MajVersion  = 1
	ProgramName = "msgrelay"
)

func main() {
	if err := config.Resolve(); err != nil {
		panic(fmt.Sprintf("Unable to resolve config: %s", err.Error()))
	}

	log.Infof("Starting %s version %d", ProgramName, MajVersion)

	metrics.Initialize("msgrelay")

	log.Infof("Initializing the pipeline")

	compsCfg, err := config_mapper.GetComponentsCfg()
	if err != nil {
		panic(fmt.Sprintf("Failed to get components config: %s", err.Error()))
	}

	pplCfg, err := config_mapper.GetPipelineCfg()
	if err != nil {
		panic(fmt.Sprintf("Failed to get pipeline config: %s", err.Error()))
	}

	pipeline, pplErr := pipeline.NewPipeline(compsCfg, pplCfg)
	if pplErr != nil {
		log.Fatalf("Failed to initialize the pipeline: %s", pplErr.Error())
	}
	log.Info("Pipeline initalization is complete")

	log.Info("Pipeline GraphViz diagram (plot using https://www.planttext.com):")
	fmt.Println(pipeline.Explain())

	log.Info("Activating the pipeline")
	startErr := pipeline.Start()
	if startErr != nil {
		log.Fatalf("Failed to start the pipeline: %s", startErr.Error())
	}
	log.Info("Pipeline successfully activated")

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	<-c
	tell.Info("Terminating the pipeline")
	stopErr := pipeline.Stop()
	if stopErr != nil {
		log.Fatalf("Failed to stop the pipeline: %s", stopErr.Error())
		os.Exit(1)
	}

	os.Exit(0)
}
