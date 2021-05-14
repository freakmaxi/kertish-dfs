package hooks

import (
	"io/fs"
	"path/filepath"
	"plugin"

	"go.uber.org/zap"
)

var CurrentLoader Loader

type Loader interface {
	List() []Action
	Get(name string) Action
}

type loader struct {
	hooksPath string
	providers map[string]Action

	logger *zap.Logger
}

var defaultHookPath = "./hooks"

func NewLoader(hooksPath *string, logger *zap.Logger) Loader {
	if hooksPath == nil {
		hooksPath = &defaultHookPath
	}
	l := &loader{
		hooksPath: *hooksPath,
		providers: make(map[string]Action),
		logger:    logger,
	}
	if err := l.load(); err != nil {
		logger.Error(
			"Hook loader unable to load any hook",
			zap.Error(err),
		)
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

		ls, err := p.Lookup("Load")
		if err != nil {
			return err
		}
		action := ls.(func() Action)()

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
