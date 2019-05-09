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

func main() {

	log.Infof("Starting %s", ProgramName)

	log.Infof("Initializing config repo")
	repo := cfg.NewRepository()
	repo.DefineSchema(cast.ConfigSchema)
	global.Store("config", repo)

	log.Infof("Registering default provider")
	if _, err := cfg.NewDefaultProvider(repo, 0); err != nil {
		log.Errorf("⚠️ Failed to register default provider: %s", err)
	}

	log.Infof("Registering env provider")
	if _, err := cfg.NewEnvProvider(repo, 10); err != nil {
		log.Errorf("⚠️ Failed to register env provider: %s", err)
	}

	if _, err := cfg.NewYamlProvider(repo, 20); err != nil {
		log.Errorf("⚠️ Failed to register yaml provider: %s", err)
	}

	log.Infof("Registering cli provider")
	if _, err := cfg.NewCliProvider(repo, 30); err != nil {
		log.Errorf("⚠️ Failed to register cli provider: %s", err)
	}

	log.Infof("Initializing config providers")
	if err := repo.SetUp(); err != nil {
		log.Errorf("⚠️ Failed to initialise config repo: %s", err)
	}

	log.Infof("Starting %s version %d, process ID: %d",
		ProgramName, MajVersion, os.Getpid())

	log.Infof("Initializing the pipeline")

	syscfgval, ok := repo.Get(types.NewKey("system"))
	if !ok {
		log.Fatalf("❌ Failed to get system config")
	}
	syscfg := syscfgval.(types.CfgBlockSystem)

	if err := metrics.Initialize(&syscfg); err != nil {
		log.Errorf("⚠️ Failed to initialize metrics module: %s", err)
	}

	compsval, ok := repo.Get(types.NewKey("components"))
	if !ok {
		log.Fatalf("❌ Failed to get components config")
	}
	compscfg := compsval.(map[string]types.CfgBlockComponent)

	pplval, ok := repo.Get(types.NewKey("pipeline"))
	if !ok {
		log.Fatalf("❌ Failed to get pipeline config")
	}
	pplcfg := pplval.(map[string]types.CfgBlockPipeline)

	pipeline, pplErr := pipeline.NewPipeline(compscfg, pplcfg)
	if pplErr != nil {
		log.Fatalf("❌ Failed to initialize the pipeline: %s", pplErr)
	}
	global.Store("pipeline", pipeline)
	log.Info("✅ Pipeline is successfully initialized")

	if explanation, err := pipeline.Explain(); err != nil {
		log.Errorf("⚠️ Failed to explain the pipeline: %s", err.Error())
	} else {
		log.Info("Pipeline GraphViz diagram (plot using https://www.planttext.com):")
		fmt.Println(explanation)
	}

	log.Info("Activating the pipeline")
	startErr := pipeline.Start()
	if startErr != nil {
		log.Fatalf("❌ Failed to start the pipeline: %s", startErr)
	}
	log.Info("✅️ Pipeline is successfully activated")

	var adminmux *webapp.HttpMux
	if syscfg.Admin.Enabled {
		var err error
		log.Infof("Starting admin interface on %s", syscfg.Admin.BindAddr)
		adminmux, err = webapp.NewHttpMux(&syscfg)
		if err != nil {
			log.Fatalf("❌ Failed to start admin interface: %s", err)
		}
	}

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	<-c
	log.Info("Terminating the daemon")

	if adminmux != nil {
		log.Info("Stopping admin interface")
		if err := adminmux.Stop(); err != nil {
			log.Errorf("⚠️ Error while stopping admin interface: %s", err.Error())
		}
		log.Infof("✅️ Done")
	}

	log.Infof("Stopping the pipeline")
	stopErr := pipeline.Stop()
	if stopErr != nil {
		log.Fatalf("❌ Failed to stop the pipeline: %s", stopErr)
	}
	log.Infof("✅️ Done")

	log.Infof("Stopping the config repo")
	if repoErr := repo.TearDown(); repoErr != nil {
		log.Fatalf("❌ Failed to tear down config repo: %s", repoErr)
	}
	log.Infof("✅️ Done")

	os.Exit(0)
}
