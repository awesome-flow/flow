package main

import (
	"fmt"
	"os"
	"os/signal"

	"github.com/awesome-flow/flow/pkg/cast"
	"github.com/awesome-flow/flow/pkg/types"

	"github.com/awesome-flow/flow/pkg/cfg"
	"github.com/awesome-flow/flow/pkg/global"
	"github.com/awesome-flow/flow/pkg/metrics"
	"github.com/awesome-flow/flow/pkg/pipeline"
	webapp "github.com/awesome-flow/flow/web/app"
	log "github.com/sirupsen/logrus"
)

const (
	MajVersion  = 1
	ProgramName = "flowd"
)

func errorf(format string, args ...interface{}) {
	log.Errorf("⚠️  "+format, args...)
}

func infof(format string, args ...interface{}) {
	log.Infof(format, args...)
}

func fatalf(format string, args ...interface{}) {
	log.Fatalf("❌ "+format, args...)
}

func main() {

	infof("Starting %s", ProgramName)

	infof("Initializing config repo")
	repo := cfg.NewRepository()
	repo.DefineSchema(cast.ConfigSchema)
	global.Store("config", repo)

	infof("Registering default provider")
	if _, err := cfg.NewDefaultProvider(repo, 0); err != nil {
		errorf("Failed to register default provider: %s", err)
	}

	infof("Registering env provider")
	if _, err := cfg.NewEnvProvider(repo, 10); err != nil {
		errorf("Failed to register env provider: %s", err)
	}

	if _, err := cfg.NewYamlProvider(repo, 20); err != nil {
		errorf("Failed to register yaml provider: %s", err)
	}

	infof("Registering cli provider")
	if _, err := cfg.NewCliProvider(repo, 30); err != nil {
		errorf("Failed to register cli provider: %s", err)
	}

	infof("Initializing config providers")
	if err := repo.SetUp(); err != nil {
		errorf("Failed to initialise config repo: %s", err)
	}

	infof("Starting %s version %d, process ID: %d",
		ProgramName, MajVersion, os.Getpid())

	infof("Initializing the pipeline")

	syscfgval, ok := repo.Get(types.NewKey("system"))
	if !ok {
		fatalf("Failed to get system config")
	}
	syscfg := syscfgval.(types.CfgBlockSystem)

	if err := metrics.Initialize(&syscfg); err != nil {
		errorf("Failed to initialize metrics module: %s", err)
	}

	compsval, ok := repo.Get(types.NewKey("components"))
	if !ok {
		fatalf("Failed to get components config")
	}
	compscfg := compsval.(map[string]types.CfgBlockActor)

	pplval, ok := repo.Get(types.NewKey("pipeline"))
	if !ok {
		fatalf("Failed to get pipeline config")
	}
	pplcfg := pplval.(map[string]types.CfgBlockPipeline)

	pipeline, pplErr := pipeline.NewPipeline(compscfg, pplcfg)
	if pplErr != nil {
		fatalf("Failed to initialize the pipeline: %s", pplErr)
	}
	global.Store("pipeline", pipeline)
	infof("Pipeline is successfully initialized")

	if explanation, err := pipeline.Explain(); err != nil {
		errorf("Failed to explain the pipeline: %s", err.Error())
	} else {
		infof("Pipeline GraphViz diagram (plot using https://www.planttext.com):")
		fmt.Println(explanation)
	}

	infof("Activating the pipeline")
	startErr := pipeline.Start()
	if startErr != nil {
		fatalf("Failed to start the pipeline: %s", startErr)
	}
	infof("Pipeline is successfully activated")

	var adminmux *webapp.HttpMux
	if syscfg.Admin.Enabled {
		var err error
		infof("Starting admin interface on %s", syscfg.Admin.BindAddr)
		adminmux, err = webapp.NewHttpMux(&syscfg)
		if err != nil {
			fatalf("Failed to start admin interface: %s", err)
		}
	}

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	<-c
	infof("Terminating the daemon")

	if adminmux != nil {
		log.Info("Stopping admin interface")
		if err := adminmux.Stop(); err != nil {
			errorf("Error while stopping admin interface: %s", err.Error())
		}
		infof("Done")
	}

	infof("Stopping the pipeline")
	stopErr := pipeline.Stop()
	if stopErr != nil {
		fatalf("Failed to stop the pipeline: %s", stopErr)
	}
	infof("Done")

	infof("Stopping the config repo")
	if repoErr := repo.TearDown(); repoErr != nil {
		fatalf("Failed to tear down config repo: %s", repoErr)
	}
	infof("Done")

	os.Exit(0)
}
