package manager

import (
	"os"
	"strings"

	"github.com/freakmaxi/kertish-dfs/basics/common"
	"github.com/freakmaxi/kertish-dfs/basics/errors"
)

func (d *dfs) Delete(target string, killZombies bool) error {
	if err := d.deleteFolder(target, killZombies); err != nil {
		if err != os.ErrNotExist {
			return err
		}
		return d.deleteFile(target, killZombies)
	}
	return nil
}

func (d *dfs) deleteFolder(folderPath string, killZombies bool) error {
	parentPath, pathName := common.Split(folderPath)

	return d.metadata.SaveBlock([]string{parentPath}, func(folders map[string]*common.Folder) (bool, error) {
		folder := folders[parentPath]
		if folder == nil {
			return false, os.ErrNotExist
		}

		return true, folder.DeleteFolder(pathName, func(fullPath string) error {
			return d.deleteFolderContent(fullPath, killZombies, folders)
		})
	})
}

func (d *dfs) deleteFolderContent(fullPath string, killZombies bool, foldersCache map[string]*common.Folder) error {
	deletingFolders, err := d.metadata.Tree(fullPath, true, true)
	if err != nil {
		if err == os.ErrNotExist {
			return errors.ErrRepair
		}
		return err
	}

	searchForFolderFunc := func(fullPath string) *common.Folder {
		for _, folder := range deletingFolders {
			if strings.Compare(folder.Full, fullPath) == 0 {
				return folder
			}
		}
		return nil
	}

	for _, folder := range deletingFolders {
		if folder.Locked() {
			return errors.ErrLock
		}

		for len(folder.Files) > 0 {
			file := folder.Files[0]

			if err := folder.DeleteFile(file.Name, func(file *common.File) error {
				return d.deleteFileChunks(file, killZombies)
			}); err != nil {
				return err
			}
		}

		p, _ := common.Split(folder.Full)
		changedFolder := searchForFolderFunc(p)
		if changedFolder != nil {
			_ = changedFolder.DeleteFolder(folder.Name, func(fullPath string) error {
				return nil
			})
			foldersCache[changedFolder.Full] = changedFolder
		}

		foldersCache[folder.Full] = nil
	}

	return nil
}

func (d *dfs) deleteFile(path string, killZombies bool) error {
	folderPath, filename := common.Split(path)

	return d.metadata.SaveBlock([]string{folderPath}, func(folders map[string]*common.Folder) (bool, error) {
		folder := folders[folderPath]
		if folder == nil {
			return false, os.ErrNotExist
		}

		return true, folder.DeleteFile(filename, func(file *common.File) error {
			if file.Locked() {
				return errors.ErrLock
			}
			return d.deleteFileChunks(file, killZombies)
		})
	})
}

func (d *dfs) deleteFileChunks(file *common.File, killZombies bool) error {
	deletionResult, err := d.cluster.Delete(file.Chunks)
	if deletionResult != nil {
		file.IngestDeletion(*deletionResult)
	}
	if err != nil && err == errors.ErrZombie {
		file.Zombie = true
	}

	if file.Zombie {
		if killZombies {
			if file.CanDie() {
				return nil
			}
			return errors.ErrZombieAlive
		}
		return errors.ErrZombie
	}

	return err
}
