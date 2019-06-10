package corev1alpha1

import "os"

type Context struct {
	logger *Logger
	config *Config
}

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
