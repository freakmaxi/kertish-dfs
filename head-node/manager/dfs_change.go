package manager

import (
	"os"
	"strings"

	"github.com/freakmaxi/kertish-dfs/basics/common"
	"github.com/freakmaxi/kertish-dfs/basics/errors"
	"github.com/freakmaxi/kertish-dfs/hooks"
)

func (d *dfs) Change(sources []string, target string, join bool, overwrite bool, move bool) error {
	if len(sources) > 1 && !join {
		return os.ErrInvalid
	}
	if err := d.changeFolder(sources, target, move); err != nil {
		if err != os.ErrNotExist {
			return err
		}
		return d.changeFile(sources, target, overwrite, move)
	}
	return nil
}

func (d *dfs) changeFolder(sources []string, target string, move bool) error {
	sources = common.CorrectPaths(sources)
	target = common.CorrectPath(target)

	sourceFolders, err := d.metadata.Get(sources)
	if err != nil {
		return err
	}

	joinedFolder, err := common.CreateJoinedFolder(sourceFolders)
	if err != nil {
		return err
	}

	clonedFolderPaths := make([]string, 0)
	clonedFoldersMap := make(map[string]*common.Folder)
	createShadowChunks := make(common.DataChunks, 0)

	for i := 0; i < len(sourceFolders); i++ {
		sourceFolder := sourceFolders[i]

		sourceChildren, err := d.metadata.ChildrenTree(sourceFolder.Full, false, false)
		if err != nil {
			return err
		}

		for j := 0; j < len(sourceChildren); j++ {
			sourceChild := sourceChildren[j]
			sourceChild.Full = strings.Replace(sourceChild.Full, sourceFolder.Full, target, 1)

			if existsClone, has := clonedFoldersMap[sourceChild.Full]; has {
				joinedChildFolder, err := common.CreateJoinedFolder([]*common.Folder{sourceChild, existsClone})
				if err != nil {
					return err
				}
				joinedChildFolder.CloneInto(sourceChild)
			}

			for i := 0; i < len(sourceChild.Files); i++ {
				file := sourceChild.Files[i]

				if file.Locked() || file.ZombieCheck() {
					if move {
						if file.Locked() {
							return errors.ErrLock
						}
						return errors.ErrZombie
					}

					_ = sourceChild.DeleteFile(file.Name, func(file *common.File) error {
						return nil
					})
					i--
					continue
				}

				if move {
					continue
				}

				createShadowChunks = append(createShadowChunks, file.Chunks...)
			}

			clonedFolderPaths = append(clonedFolderPaths, sourceFolder.Full)
			clonedFoldersMap[sourceChild.Full] = sourceChild
		}
	}

	if err := d.metadata.SaveChain(target, func(targetFolder *common.Folder) (bool, error) {
		if len(targetFolder.Files) > 0 || len(targetFolder.Folders) > 0 {
			return false, errors.ErrNotEmpty
		}

		joinedFolder.CloneInto(targetFolder)

		for i := 0; i < len(targetFolder.Files); i++ {
			file := targetFolder.Files[i]

			if file.Locked() || file.ZombieCheck() {
				if move {
					if file.Locked() {
						return false, errors.ErrLock
					}
					return false, errors.ErrZombie
				}

				_ = targetFolder.DeleteFile(file.Name, func(file *common.File) error {
					return nil
				})
				i--
				continue
			}

			if move {
				continue
			}

			createShadowChunks = append(createShadowChunks, file.Chunks...)
		}

		return true, nil
	}); err != nil {
		return err
	}

	if move {
		// joined with clonedFolderPaths to query easily
		for _, source := range sources {
			sourceParent, _ := common.Split(source)

			clonedFolderPaths = append(clonedFolderPaths, sourceParent)
			clonedFolderPaths = append(clonedFolderPaths, source)
		}
	}

	return d.metadata.SaveBlock(clonedFolderPaths, func(folders map[string]*common.Folder) (bool, error) {
		if move {
			for _, source := range sources {
				sourceParent, sourceName := common.Split(source)

				folder, has := folders[sourceParent]
				if has {
					_ = folder.DeleteFolder(sourceName, func(_ string) error {
						return nil
					})
				}
				folders[source] = nil
			}
		}

		for k, v := range clonedFoldersMap {
			folders[k] = v
		}

		if move {
			// Handle Hooks
			for _, source := range sources {
				actions := d.compileHookActions(source, hooks.Updated)
				d.ExecuteActions(hooks.NewActionInfoForMovedFolder(source, target), actions)
			}

			return true, nil
		}

		if err := d.cluster.CreateShadow(createShadowChunks); err != nil {
			return false, err
		}

		// Handle Hooks
		for _, source := range sources {
			actions := d.compileHookActions(source, hooks.Updated)
			d.ExecuteActions(hooks.NewActionInfoForCopiedFolder(source, target), actions)
		}

		return true, nil
	})
}

func (d *dfs) changeFile(sources []string, target string, overwrite bool, move bool) error {
	targetParent, targetFilename := common.Split(target)

	targetFolders, err := d.metadata.Get([]string{targetParent})
	if err != nil && err != os.ErrNotExist {
		return err
	}

	if targetFolders != nil {
		targetFile := targetFolders[0].File(targetFilename)
		if targetFile != nil {
			if !overwrite {
				return os.ErrExist
			}

			if err := d.deleteFile(target, false); err != nil {
				return err
			}
		}
	}

	sourceParents := make([]string, 0)
	sourceFoldersMap := make(map[string]*common.Folder)
	sourceFiles := make(common.Files, 0)

	for _, source := range sources {
		sourceParent, sourceFilename := common.Split(source)

		sourceFolder, has := sourceFoldersMap[sourceParent]
		if !has {
			sourceFolders, err := d.metadata.Get([]string{sourceParent})
			if err != nil {
				return err
			}
			sourceFolder = sourceFolders[0]
			sourceFoldersMap[sourceParent] = sourceFolder
			sourceParents = append(sourceParents, sourceParent)
		}

		sourceFile := sourceFolder.File(sourceFilename)
		if sourceFile == nil {
			return os.ErrNotExist
		}

		if sourceFile.Locked() {
			return errors.ErrLock
		}

		if sourceFile.ZombieCheck() {
			return errors.ErrZombie
		}

		sourceFiles = append(sourceFiles, sourceFile)
	}

	joinedFile, err := common.CreateJoinedFile(sourceFiles)
	if err != nil {
		return err
	}

	if err := d.metadata.SaveChain(targetParent, func(targetFolder *common.Folder) (bool, error) {
		targetFile, err := targetFolder.NewFile(targetFilename)
		if err != nil {
			return false, err
		}
		targetFile.Reset(joinedFile.Mime, joinedFile.Size)
		joinedFile.CloneInto(targetFile)

		if !move {
			if err := d.cluster.CreateShadow(targetFile.Chunks); err != nil {
				return false, err
			}
		}
		targetFile.Lock.Cancel()

		return true, nil
	}); err != nil {
		return err
	}

	if !move {
		// Handle Hooks
		for _, source := range sources {
			actions := d.compileHookActions(source, hooks.Updated)
			d.ExecuteActions(hooks.NewActionInfoForMovedFile(source, target, overwrite), actions)
		}

		return nil
	}

	return d.metadata.SaveBlock(sourceParents, func(folders map[string]*common.Folder) (bool, error) {
		for _, source := range sources {
			sourceParent, sourceFilename := common.Split(source)
			sourceFolder := folders[sourceParent]

			_ = sourceFolder.DeleteFile(sourceFilename, func(file *common.File) error {
				return nil
			})
		}

		// Handle Hooks
		for _, source := range sources {
			actions := d.compileHookActions(source, hooks.Updated)
			d.ExecuteActions(hooks.NewActionInfoForCopiedFile(source, target, overwrite), actions)
		}

		return true, nil
	})
}
