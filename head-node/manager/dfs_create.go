package manager

import (
	"io"
	"os"

	"github.com/freakmaxi/kertish-dfs/basics/common"
	"github.com/freakmaxi/kertish-dfs/basics/errors"
	"go.uber.org/zap"
)

func (d *dfs) CreateFolder(folderPath string) error {
	folderPath = common.CorrectPath(folderPath)

	return d.metadata.SaveChain(folderPath, func(folder *common.Folder) (bool, error) {
		return true, nil
	})
}

func (d *dfs) CreateFile(path string, mime string, size uint64, overwrite bool, contentReader io.Reader) error {
	path = common.CorrectPath(path) // It is required in here to eliminate wrong path format

	folderPath, filename := common.Split(path)
	if len(filename) == 0 {
		return os.ErrInvalid
	}

	var file *common.File

	if err := d.metadata.SaveChain(folderPath, func(folder *common.Folder) (bool, error) {
		var err error

		file = folder.File(filename)
		if file == nil {
			file, err = folder.NewFile(filename)
			return true, err
		}

		if !overwrite {
			return false, os.ErrExist
		}

		if file.Locked() {
			return false, errors.ErrLock
		}

		file.Lock = common.NewFileLockForSize(size)

		deletionResult, err := d.cluster.Delete(file.Chunks)
		if deletionResult != nil {
			file.IngestDeletion(*deletionResult)
		}

		if err != nil {
			if err != errors.ErrZombie {
				file.Lock.Cancel()
				return true, err
			}
			file.Zombie = true
		}

		return true, nil
	}); err != nil {
		return err
	}

	chunks, err := d.cluster.Create(size, contentReader)
	if err != nil {
		if errUpdate := d.update(path, nil); errUpdate != nil {
			d.logger.Error(
				"Dropping file entry due to file creation failure is failed, file is now zombie",
				zap.String("path", path),
				zap.Error(errUpdate),
			)
		}
		return err
	}

	file.Reset(mime, size)
	file.Chunks = append(file.Chunks, chunks...)
	file.Lock.Cancel()

	err = d.update(path, file)
	if err != nil {
		d.logger.Error(
			"Saving file creation is failed. File is now zombie with orphan chunks in data node! Run repair to eliminate",
			zap.String("path", path),
			zap.Error(err),
		)
	}
	return err
}

func (d *dfs) update(folderPath string, file *common.File) error {
	parent, filename := common.Split(folderPath)

	return d.metadata.SaveBlock([]string{parent}, func(folders map[string]*common.Folder) (bool, error) {
		folder := folders[parent]
		if folder == nil {
			return false, os.ErrNotExist
		}
		folder.ReplaceFile(filename, file)
		return true, nil
	})
}
