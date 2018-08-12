package main

import (
	"fmt"
	"os"
	"os/signal"

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

	tell.Init()

	tell.Infof("Starting %s version %d", ProgramName, MajVersion)

	bmetrics.Initialize("", "msgrelay")

	tell.Infof("Initializing the pipeline")

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
		tell.Fatalf("Failed to initialize the pipeline: %s", pplErr.Error())
	}
	tell.Info("Pipeline initalization is complete")

	tell.Info("Pipeline GraphViz diagram (plot using https://www.planttext.com):")
	fmt.Println(pipeline.Explain())

	tell.Info("Activating the pipeline")
	startErr := pipeline.Start()
	if startErr != nil {
		tell.Fatalf("Failed to start the pipeline: %s", startErr.Error())
	}
	tell.Info("Pipeline successfully activated")

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	<-c
	tell.Info("Terminating the pipeline")
	stopErr := pipeline.Stop()
	if stopErr != nil {
		tell.Fatalf("Failed to stop the pipeline: %s", stopErr.Error())
		os.Exit(1)
	}

	os.Exit(0)
}
