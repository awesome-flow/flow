package plugin

import (
	"fmt"
	"os"
	goplugin "plugin"
)

type Symbol interface{}

type Plugin interface {
	Load() error
	Lookup(string) (Symbol, error)
}

type GoPlugin struct {
	path string
	plug *goplugin.Plugin
}

var _ Plugin = (*GoPlugin)(nil)

func NewGoPlugin(path string) *GoPlugin {
	return &GoPlugin{
		path: path,
	}
}

func (g *GoPlugin) Load() error {
	if _, err := os.Stat(g.path); err != nil {
		if os.IsNotExist(err) {
			return fmt.Errorf("failed to load plugin shared library from %s", g.path)
		}
		return err
	}
	p, err := goplugin.Open(g.path)
	if err != nil {
		return err
	}
	g.plug = p
	return nil
}

func (g *GoPlugin) Lookup(symName string) (Symbol, error) {
	return g.plug.Lookup(symName)
}

type Loader func(string) (Plugin, error)

func GoPluginLoader(path string) (Plugin, error) {
	p := NewGoPlugin(path)
	if err := p.Load(); err != nil {
		return nil, err
	}

	return p, nil
}
