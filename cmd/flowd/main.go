package main

import (
	"fmt"
	"os"
	"os/signal"

	"github.com/awesome-flow/flow/pkg/cast"

	"github.com/awesome-flow/flow/pkg/admin"
	"github.com/awesome-flow/flow/pkg/cfg"
	"github.com/awesome-flow/flow/pkg/config"
	config_mapper "github.com/awesome-flow/flow/pkg/config/mapper"
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

	/* ======== EXPERIMENT SECTION ========= */

	log.Infof("Initialising the repo")
	repo := cfg.NewRepository()
	repo.DefineSchema(cast.ConfigSchema)

	log.Infof("Registering default provider")
	if _, err := cfg.NewDefaultProvider(repo, 0); err != nil {
		log.Errorf("Failed to register default provider: %s", err)
	}

	log.Infof("Registering env provider")
	if _, err := cfg.NewEnvProvider(repo, 10); err != nil {
		log.Errorf("Failed to register env provider: %s", err)
	}

	if cfgpath, ok := config.Get("config.file"); ok {
		log.Infof("Registering yaml provider with source %s", cfgpath.(string))
		if _, err := cfg.NewYamlProviderFromSource(repo, 20, cfgpath.(string), &cfg.YamlProviderOptions{}); err != nil {
			log.Errorf("Failed to register yaml provider: %s", err)
		}
	}

	log.Infof("Registering cli provider")
	if _, err := cfg.NewCliProvider(repo, 30); err != nil {
		log.Errorf("Failed to register cli provider: %s", err)
	}

	log.Infof("Initializing repo providers")
	if err := repo.SetUp(); err != nil {
		log.Errorf("Failed to initialise config repo: %s", err)
	}

	if syscfg, ok := repo.Get(cast.NewKey("system")); ok {
		log.Infof("system config: %#v", syscfg)
	} else {
		log.Errorf("Expected to get system config from repo, got none")
	}

	if compcfg, ok := repo.Get(cast.NewKey("components")); ok {
		log.Infof("components config: %#v", compcfg)
	} else {
		log.Errorf("Expected to get components config from repo, got none")
	}

	if pplcfg, ok := repo.Get(cast.NewKey("pipeline")); ok {
		log.Infof("pipeline config: %#v", pplcfg)
	} else {
		log.Errorf("Expected to get pipeline config from repo, got none")
	}
	/* ===== END OF EXPERIMENT SECTION ===== */

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

	if explanation, err := pipeline.Explain(); err != nil {
		log.Errorf("Failed to explain the pipeline: %s", err.Error())
	} else {
		log.Info("Pipeline GraphViz diagram (plot using https://www.planttext.com):")
		fmt.Println(explanation)
	}

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
