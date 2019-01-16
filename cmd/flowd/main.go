package main

import (
	"fmt"
	"os"
	"os/signal"

	"github.com/awesome-flow/flow/pkg/admin"
	"github.com/awesome-flow/flow/pkg/config"
	"github.com/awesome-flow/flow/pkg/config/mapper"
	"github.com/awesome-flow/flow/pkg/global"
	"github.com/awesome-flow/flow/pkg/metrics"
	"github.com/awesome-flow/flow/pkg/pipeline"
	log "github.com/sirupsen/logrus"
)

const (
	MajVersion  = 1
	ProgramName = "flowd"
)

func main() {
	if err := config.Resolve(); err != nil {
		panic(fmt.Sprintf("Unable to resolve config: %s", err))
	}

	log.Infof("Starting %s version %d, process ID: %d",
		ProgramName, MajVersion, os.Getpid())

	log.Infof("Initializing the pipeline")

	sysCfg, err := config_mapper.GetSystemCfg()
	if err != nil {
		panic(fmt.Sprintf("Failed to get system config: %s", err))
	}

	if err := metrics.Initialize(sysCfg); err != nil {
		log.Errorf("Failed to initialize metrics module: %s\n", err)
	}

	compsCfg, err := config_mapper.GetComponentsCfg()
	if err != nil {
		panic(fmt.Sprintf("Failed to get components config: %s", err))
	}

	pplCfg, err := config_mapper.GetPipelineCfg()
	if err != nil {
		panic(fmt.Sprintf("Failed to get pipeline config: %s", err))
	}

	pipeline, pplErr := pipeline.NewPipeline(compsCfg, pplCfg)
	if pplErr != nil {
		log.Fatalf("Failed to initialize the pipeline: %s", pplErr)
	}
	global.Store("pipeline", pipeline)
	log.Info("Pipeline initalization is complete")

	log.Info("Pipeline GraphViz diagram (plot using https://www.planttext.com):")
	fmt.Println(pipeline.Explain())

	log.Info("Activating the pipeline")
	startErr := pipeline.Start()
	if startErr != nil {
		log.Fatalf("Failed to start the pipeline: %s", startErr)
	}
	log.Info("Pipeline successfully activated")

	var adminmux *admin.HttpMux
	if sysCfg.Admin.Enabled {
		log.Infof("Starting admin interface on %s", sysCfg.Admin.BindAddr)
		adminmux, err = admin.NewHttpMux(sysCfg)
		if err != nil {
			log.Fatalf("Failed to start admin interface: %s", err)
		}
	}

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	<-c
	log.Info("Terminating the pipeline")
	if adminmux != nil {
		log.Info("Stopping admin interface")
		if err := adminmux.Stop(); err != nil {
			log.Errorf("Error while stopping admin interface: %s", err.Error())
		}
	}
	stopErr := pipeline.Stop()
	if stopErr != nil {
		log.Fatalf("Failed to stop the pipeline: %s", stopErr)
		os.Exit(1)
	}

	os.Exit(0)
}
