package corev1alpha1

import (
	"os"

	"github.com/awesome-flow/flow/pkg/util"
)

type Context struct {
	logger *Logger
	config *Config
}

var _ Runner = (*Context)(nil)

func NewContext(config *Config) (*Context, error) {
	logger, err := initLogger(config)
	if err != nil {
		return nil, err
	}
	return &Context{
		logger: logger,
		config: config,
	}, nil
}

func initLogger(config *Config) (*Logger, error) {
	//TODO: configure logger
	return NewLogger(os.Stdout), nil
}

func (ctx *Context) Start() error {
	if err := util.ExecEnsure(
		ctx.logger.Start,
		ctx.config.Start,
	); err != nil {
		return err
	}
	return nil
}

func (ctx *Context) Stop() error {
	if err := util.ExecEnsure(
		ctx.logger.Stop,
		ctx.config.Stop,
	); err != nil {
		return err
	}
	return nil
}

func (ctx *Context) Logger() *Logger {
	return ctx.logger
}

func (ctx *Context) Config() *Config {
	return ctx.config
}
