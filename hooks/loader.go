package hooks

import (
	"fmt"
	"io/fs"
	"path/filepath"
	"plugin"
)

type Loader interface {
	List() []Action
	Get(name string) Action
}

type loader struct {
	hooksPath string
	providers map[string]Action
}

var CurrentLoader = newLoader()

func newLoader() Loader {
	l := &loader{
		hooksPath: "./hooks",
		providers: make(map[string]Action),
	}
	if err := l.load(); err != nil {
		fmt.Println("hook loader unable to load any hook")
	}
	return l
}

func (l *loader) load() error {
	return filepath.Walk(l.hooksPath, func(path string, info fs.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}

		p, err := plugin.Open(path)
		if err != nil {
			return err
		}

		ns, err := p.Lookup("Name")
		if err != nil {
			return err
		}
		name := ns.(func() string)()

		as, err := p.Lookup(name)
		if err != nil {
			return err
		}
		action := as.(Action)

		l.providers[name] = action

		return nil
	})
}

func (l *loader) List() []Action {
	actions := make([]Action, 0)
	for _, action := range l.providers {
		actions = append(actions, action)
	}
	return actions
}

func (l *loader) Get(name string) Action {
	a, has := l.providers[name]
	if !has {
		return nil
	}
	return a
}

var _ Loader = &loader{}
