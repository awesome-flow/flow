package main

import (
	"fmt"
	"os"
	"os/signal"

	"github.com/awesome-flow/flow/pkg/cast"
	"github.com/awesome-flow/flow/pkg/cfg"
	core "github.com/awesome-flow/flow/pkg/corev1alpha1"
	pipeline "github.com/awesome-flow/flow/pkg/corev1alpha1/pipeline"
	"github.com/awesome-flow/flow/pkg/types"
	"github.com/awesome-flow/flow/pkg/util"
	webapp "github.com/awesome-flow/flow/web/app"
)

func main() {
	repo := cfg.NewRepository()
	repo.DefineSchema(cast.ConfigSchema)

	if err := util.ExecEnsure(
		func() error { _, err := cfg.NewDefaultProvider(repo, 0); return err },
		func() error { _, err := cfg.NewEnvProvider(repo, 10); return err },
		func() error { _, err := cfg.NewYamlProvider(repo, 20); return err },
		func() error { _, err := cfg.NewCliProvider(repo, 30); return err },
	); err != nil {
		panic(fmt.Sprintf("config init failed: %s", err.Error()))
	}

	config := core.NewConfig(repo)
	context, err := core.NewContext(config)
	if err != nil {
		panic(fmt.Sprintf("failed to init context: %s", err))
	}

	if err := context.Start(); err != nil {
		panic(fmt.Sprintf("failed to start context: %s", err))
	}

	logger := context.Logger()

	logger.Info("initializing the pipeline")
	pipeline, err := pipeline.NewPipeline(context)
	if err != nil {
		logger.Fatal("failed to init pipeline: %s", err)
	}
	logger.Info("pipeline was initialized")

	logger.Info("starting the pipeline")
	if err := pipeline.Start(); err != nil {
		logger.Fatal("failed to start pipeline: %s", err)
	}
	logger.Info("pipeline is active")

	syscfgval, ok := repo.Get(types.NewKey("system"))
	if !ok {
		logger.Fatal("failed to get system config")
	}
	syscfg := syscfgval.(types.CfgBlockSystem)
	var adminmux *webapp.HttpMux
	if syscfg.Admin.Enabled {
		var err error
		logger.Info("starting admin interface on %s", syscfg.Admin.Bind)
		adminmux, err = webapp.NewHttpMux(context)
		if err != nil {
			logger.Fatal("failed to start admin interface: %s", err)
		}
	}

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	<-c

	logger.Info("terminating")

	if adminmux != nil {
		logger.Info("stopping admin interface")
		if err := adminmux.Stop(); err != nil {
			logger.Error("failed to stop admin interface: %s", err.Error())
		} else {
			logger.Info("admin interface was successfully terminated")
		}
	}

	if err := util.ExecEnsure(
		pipeline.Stop,
		context.Stop,
	); err != nil {
		logger.Fatal("failed to terminate pipeline: %s", err)
	}

	os.Exit(0)
}
