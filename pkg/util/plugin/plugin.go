package plugin

import (
	"fmt"
	"os"
	"path"
	goplugin "plugin"
)

type Symbol interface{}

type Plugin interface {
	Load() error
	Lookup(string) (Symbol, error)
}

type GoPlugin struct {
	path string
	name string
	plug *goplugin.Plugin
}

var _ Plugin = (*GoPlugin)(nil)

func NewGoPlugin(path, name string) *GoPlugin {
	return &GoPlugin{
		path: path,
		name: name,
	}
}

func (g *GoPlugin) Load() error {
	fullpath := path.Join(g.path, g.name, g.name+".so")
	if _, err := os.Stat(fullpath); err != nil {
		if os.IsNotExist(err) {
			return fmt.Errorf("failed to load plugin .so library from %s", fullpath)
		}
		return err
	}
	p, err := goplugin.Open(fullpath)
	if err != nil {
		return err
	}
	g.plug = p

	return nil
}

func (g *GoPlugin) Lookup(symName string) (Symbol, error) {
	return g.plug.Lookup(symName)
}

type Loader func(path, name string) (Plugin, error)

func GoPluginLoader(path, name string) (Plugin, error) {
	p := NewGoPlugin(path, name)
	if err := p.Load(); err != nil {
		return nil, err
	}

	return p, nil
}
