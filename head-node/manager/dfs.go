package manager

import (
	"io"
	"strings"

	"github.com/freakmaxi/kertish-dfs/basics/hooks"
	"github.com/freakmaxi/kertish-dfs/head-node/data"
	"go.uber.org/zap"
)

// Dfs interface is for file manipulation operations base on REST service request
type Dfs interface {
	CreateFolder(folderPath string) error
	CreateFile(path string, mime string, size uint64, overwrite bool, contentReader io.Reader) error

	Read(paths []string, join bool) (ReadContainer, error)
	Size(folderPath string) (uint64, error)

	Change(sources []string, target string, join bool, overwrite bool, move bool) error

	Delete(path string, killZombies bool) error

	// ExecuteActions executes the hook actions in sync manner
	ExecuteActions(aI *hooks.ActionInfo, actions []hooks.Action)
}

type dfs struct {
	metadata data.Metadata
	cluster  Cluster
	logger   *zap.Logger
}

// NewDfs creates the instance of file manipulation operations object for REST service request
func NewDfs(metadata data.Metadata, cluster Cluster, logger *zap.Logger) Dfs {
	return &dfs{
		metadata: metadata,
		cluster:  cluster,
		logger:   logger,
	}
}

func (d *dfs) ExecuteActions(aI *hooks.ActionInfo, actions []hooks.Action) {
	if len(actions) == 0 || aI == nil {
		return
	}

	for _, action := range actions {
		if err := action.Execute(aI); err != nil {
			d.logger.Warn(
				"Execution of the hook action is failed",
				zap.String("action", aI.Action),
				zap.String("sourcePath", aI.SourcePath),
				zap.Stringp("targetPath", aI.TargetPath),
				zap.Bool("folder", aI.Folder),
				zap.Error(err),
			)
		}
	}
}

func (d *dfs) compileHookActions(folderPath string, actionType hooks.RunOn) []hooks.Action {
	actions := make([]hooks.Action, 0)
	folders, err := d.metadata.ParentTree(folderPath, true, false)
	if err != nil {
		return make([]hooks.Action, 0)
	}

	for _, folder := range folders {
		if len(folder.Hooks) == 0 {
			continue
		}

		for _, hook := range folder.Hooks {
			if hook.RunOn != hooks.All && hook.RunOn != actionType {
				continue
			}
			if strings.Compare(folderPath, folder.Full) != 0 && !hook.Recursive {
				continue
			}

			action, err := hook.Action()
			if err != nil {
				d.logger.Error(
					"Hook action creation is failed on the hook",
					zap.Error(err),
				)
				continue
			}
			actions = append(actions, action)
		}
	}

	return actions
}

var _ Dfs = &dfs{}
