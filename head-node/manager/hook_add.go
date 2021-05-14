package manager

import (
	"github.com/freakmaxi/kertish-dfs/basics/common"
	"github.com/freakmaxi/kertish-dfs/basics/hooks"
	"go.uber.org/zap"
)

func (h *hook) Add(folderPaths []string, hook *hooks.Hook) error {
	folderPaths = common.CorrectPaths(folderPaths)

	return h.metadata.SaveBlock(folderPaths, func(folders map[string]*common.Folder) (bool, error) {
		hasChanges := false

		for _, folderPath := range folderPaths {
			folder := folders[folderPath]
			if folder == nil {
				h.logger.Warn(
					"Unable to add hook because folder is not exists or it is a file",
					zap.String("hookPath", folderPath),
				)
				continue
			}

			if folder.Hooks == nil {
				folder.Hooks = make(hooks.Hooks, 0)
			}
			folder.Hooks = append(folder.Hooks, hook)
			hasChanges = true
		}

		return hasChanges, nil
	})
}
