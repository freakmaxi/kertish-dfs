package manager

import (
	"os"
	"strings"

	"github.com/freakmaxi/kertish-dfs/basics/common"
)

func (h *hook) Delete(folderPath string, hookIds []string) error {
	folderPath = common.CorrectPath(folderPath)

	return h.metadata.SaveBlock([]string{folderPath}, func(folders map[string]*common.Folder) (bool, error) {
		folder := folders[folderPath]
		if folder == nil {
			return false, os.ErrNotExist
		}
		if folder.Hooks == nil {
			return false, nil
		}

		hasChanges := false

		for len(hookIds) > 0 {
			hookId := hookIds[0]

			for i := 0; i < len(folder.Hooks); i++ {
				if strings.Compare(folder.Hooks[i].Id, hookId) != 0 {
					continue
				}
				folder.Hooks = append(folder.Hooks[:i], folder.Hooks[i+1:]...)
				hasChanges = true
				break
			}

			hookIds = hookIds[1:]
		}

		return hasChanges, nil
	})
}
