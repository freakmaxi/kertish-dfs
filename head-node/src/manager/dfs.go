package manager

import (
	"io"
	"os"
	"strings"

	"github.com/freakmaxi/kertish-dfs/head-node/src/common"
	"github.com/freakmaxi/kertish-dfs/head-node/src/data"
	"github.com/freakmaxi/kertish-dfs/head-node/src/errors"
)

type Dfs interface {
	CreateFolder(folderPath string) error
	CreateFile(path string, mime string, size uint64, contentReader io.Reader, overwrite bool) error

	Read(path string,
		folderHandler func(folder *common.Folder) error,
		fileHandler func(file *common.File, streamHandler func(writer io.Writer, begins int64, ends int64) error) error) error
	Size(folderPath string) (uint64, error)

	Move(source string, target string, overwrite bool) error
	Copy(source string, target string, overwrite bool) error
	Delete(path string) error
}

type dfs struct {
	metadata data.Metadata
	cluster  Cluster
}

func NewDfs(metadata data.Metadata, cluster Cluster) Dfs {
	return &dfs{
		metadata: metadata,
		cluster:  cluster,
	}
}

func (d *dfs) CreateFolder(folderPath string) error {
	folderPath = common.CorrectPath(folderPath)
	folderTree := common.PathTree(folderPath)

	return d.metadata.Save(folderTree, func(folders map[string]*common.Folder) error {
		folder := folders[folderPath]
		if folder != nil {
			return os.ErrExist
		}

		if _, err := d.createFolderTree(folderPath, folders); err != nil {
			return err
		}
		return nil
	})
}

// this func is modified to prevent the locking whole folder path tree.
func (d *dfs) CreateFile(path string, mime string, size uint64, contentReader io.Reader, overwrite bool) error {
	path = common.CorrectPath(path) // It is required in here

	folderPath, filename := common.Split(path)
	if len(filename) == 0 {
		return os.ErrInvalid
	}
	folderTree := common.PathTree(folderPath)

	var folder *common.Folder
	var file *common.File

	if err := d.metadata.Save(folderTree, func(folders map[string]*common.Folder) error {
		var err error

		folder = folders[folderPath]
		if folder == nil {
			folder, err = d.createFolderTree(folderPath, folders)
			if err != nil {
				return err
			}
		}

		file = folder.File(filename)
		if file != nil {
			if !overwrite {
				return os.ErrExist
			}
			file.Locked = true
			return nil
		}

		file, err = folder.NewFile(filename)
		return err
	}); err != nil {
		return err
	}

	if overwrite && len(file.Chunks) > 0 {
		if err := d.cluster.Delete(file.Chunks); err != nil {
			file.Locked = false
			if err := d.update(path, file); err != nil {
				return err
			}
			return err
		}
	}

	chunks, err := d.cluster.Create(size, contentReader)
	if err != nil {
		if err := d.update(path, nil); err != nil {
			return err
		}
		return err
	}

	file.Reset(mime, size)
	file.Chunks = append(file.Chunks, chunks...)
	file.Locked = false

	return d.update(path, file)
}

func (d *dfs) Read(path string,
	folderHandler func(folder *common.Folder) error,
	fileHandler func(file *common.File, streamHandler func(writer io.Writer, begins int64, ends int64) error) error) error {

	if err := d.folder(path, folderHandler); err != nil {
		if err != os.ErrNotExist {
			return err
		}
		return d.file(path, fileHandler)
	}
	return nil
}

func (d *dfs) Size(folderPath string) (uint64, error) {
	folderPath = common.CorrectPath(folderPath)
	size := uint64(0)

	if err := d.metadata.Lock([]string{folderPath}, func(folders []*common.Folder) error {
		for _, file := range folders[0].Files {
			if file.Locked {
				continue
			}
			size += file.Size
		}

		return d.metadata.LockChildrenOf(folders[0].Full, func(folders []*common.Folder) error {
			for _, folder := range folders {
				for _, file := range folder.Files {
					if file.Locked {
						continue
					}
					size += file.Size
				}
			}
			return nil
		})
	}); err != nil {
		return 0, err
	}
	return size, nil
}

func (d *dfs) folder(folderPath string, folderHandler func(folder *common.Folder) error) error {
	folderPath = common.CorrectPath(folderPath)

	return d.metadata.Lock([]string{folderPath}, func(folders []*common.Folder) error {
		return folderHandler(folders[0])
	})
}

func (d *dfs) file(path string, fileHandler func(file *common.File, streamHandler func(writer io.Writer, begins int64, ends int64) error) error) error {
	folderPath, filename := common.Split(path)
	if len(filename) == 0 {
		return os.ErrInvalid
	}

	return d.metadata.Lock([]string{folderPath}, func(folders []*common.Folder) error {
		file := folders[0].File(filename)
		if file == nil {
			return os.ErrNotExist
		}
		return fileHandler(file, func(writer io.Writer, begins int64, ends int64) error {
			return d.cluster.Read(file.Chunks, writer, begins, ends)
		})
	})
}

func (d *dfs) Move(source string, target string, overwrite bool) error {
	if err := d.moveFolder(source, target, overwrite); err != nil {
		if err != os.ErrNotExist {
			return err
		}
		return d.moveFile(source, target, overwrite)
	}
	return nil
}

func (d *dfs) moveFolder(source string, target string, overwrite bool) error {
	source = common.CorrectPath(source)
	target = common.CorrectPath(target)

	sourceParent, sourceChild := common.Split(source)

	folderPaths := make([]string, 0)
	folderPaths = append(folderPaths, sourceParent, source)
	folderPaths = append(folderPaths, common.PathTree(target)...)

	err := d.metadata.Save(folderPaths, func(folders map[string]*common.Folder) error {
		sourceFolder := folders[source]
		if sourceFolder == nil {
			return os.ErrNotExist
		}

		if sourceFolder.Locked() {
			return errors.ErrLock
		}

		targetFolder := folders[target]
		if targetFolder != nil {
			return os.ErrExist
		} else {
			var err error
			targetFolder, err = d.createFolderTree(target, folders)
			if err != nil {
				return err
			}
		}

		sourceFolder.CloneInto(targetFolder)

		if err := folders[sourceParent].DeleteFolder(sourceChild, func(fullPath string) error {
			folders[fullPath] = nil

			return d.metadata.LockChildrenOf(fullPath, func(sourceChildren []*common.Folder) error {
				for _, sourceChild := range sourceChildren {
					if sourceChild.Locked() {
						return errors.ErrLock
					}
					currentFull := sourceChild.Full
					sourceChild.Full = strings.Replace(sourceChild.Full, fullPath, targetFolder.Full, 1)
					folders[currentFull] = sourceChild
				}
				return nil
			})
		}); err != nil {
			return err
		}

		return nil
	})

	if err != nil {
		if overwrite && err == os.ErrExist {
			if err := d.deleteFolder(target); err != nil {
				return err
			}
			return d.moveFolder(source, target, overwrite)
		}
		return err
	}
	return nil
}

func (d *dfs) moveFile(source string, target string, overwrite bool) error {
	sourceParent, sourceFilename := common.Split(source)
	targetParent, targetFilename := common.Split(target)

	folderPaths := make([]string, 0)
	folderPaths = append(folderPaths, sourceParent)
	folderPaths = append(folderPaths, common.PathTree(targetParent)...)

	err := d.metadata.Save(folderPaths, func(folders map[string]*common.Folder) error {
		sourceFolder := folders[sourceParent]
		if sourceFolder == nil {
			return os.ErrNotExist
		}

		sourceFile := sourceFolder.File(sourceFilename)
		if sourceFile == nil {
			return os.ErrNotExist
		}

		if sourceFile.Locked {
			return errors.ErrLock
		}

		targetFolder := folders[target]
		if targetFolder == nil {
			var err error
			targetFolder, err = d.createFolderTree(targetParent, folders)
			if err != nil {
				return err
			}
		}

		targetFile := targetFolder.File(sourceFilename)
		if targetFile != nil {
			return os.ErrExist
		}

		targetFile, err := targetFolder.NewFile(targetFilename)
		if err != nil {
			return err
		}
		targetFile.Reset(sourceFile.Mime, sourceFile.Size)
		targetFile.Locked = false
		sourceFile.CloneInto(targetFile)

		return sourceFolder.DeleteFile(sourceFilename, func(file *common.File) error {
			return nil
		})
	})

	if err != nil {
		if overwrite && err == os.ErrExist {
			if err := d.deleteFile(target); err != nil {
				return err
			}
			return d.moveFile(source, target, overwrite)
		}
		return err
	}
	return nil
}

func (d *dfs) Copy(source string, target string, overwrite bool) error {
	if err := d.copyFolder(source, target, overwrite); err != nil {
		if err != os.ErrNotExist {
			return err
		}
		return d.copyFile(source, target, overwrite)
	}
	return nil
}

func (d *dfs) copyFolder(source string, target string, overwrite bool) error {
	source = common.CorrectPath(source)
	target = common.CorrectPath(target)

	folderPaths := make([]string, 0)
	folderPaths = append(folderPaths, source)
	folderPaths = append(folderPaths, common.PathTree(target)...)

	err := d.metadata.Save(folderPaths, func(folders map[string]*common.Folder) error {
		sourceFolder := folders[source]
		if sourceFolder == nil {
			return os.ErrNotExist
		}

		targetFolder := folders[target]
		if targetFolder != nil {
			return os.ErrExist
		} else {
			var err error
			targetFolder, err = d.createFolderTree(target, folders)
			if err != nil {
				return err
			}
		}

		sourceFolder.CloneInto(targetFolder)

		for _, file := range targetFolder.Files {
			if file.Locked {
				continue
			}
			if err := d.cluster.CreateShadow(file.Chunks); err != nil {
				return err
			}
		}

		return d.metadata.LockChildrenOf(sourceFolder.Full, func(sourceChildren []*common.Folder) error {
			for _, sourceChild := range sourceChildren {
				sourceChild.Full = strings.Replace(sourceChild.Full, sourceFolder.Full, targetFolder.Full, 1)
				for _, file := range sourceChild.Files {
					if file.Locked {
						continue
					}
					if err := d.cluster.CreateShadow(file.Chunks); err != nil {
						return err
					}
				}
				folders[sourceChild.Full] = sourceChild
			}
			return nil
		})
	})

	if err != nil {
		if overwrite && err == os.ErrExist {
			if err := d.deleteFolder(target); err != nil {
				return err
			}
			return d.copyFolder(source, target, overwrite)
		}
		return err
	}
	return nil
}

func (d *dfs) copyFile(source string, target string, overwrite bool) error {
	sourceParent, sourceFilename := common.Split(source)
	targetParent, targetFilename := common.Split(target)

	folderPaths := make([]string, 0)
	folderPaths = append(folderPaths, sourceParent)
	folderPaths = append(folderPaths, common.PathTree(targetParent)...)

	err := d.metadata.Save(folderPaths, func(folders map[string]*common.Folder) error {
		sourceFolder := folders[sourceParent]
		if sourceFolder == nil {
			return os.ErrNotExist
		}

		sourceFile := sourceFolder.File(sourceFilename)
		if sourceFile == nil {
			return os.ErrNotExist
		}

		if sourceFile.Locked {
			return errors.ErrLock
		}

		targetFolder := folders[target]
		if targetFolder == nil {
			var err error
			targetFolder, err = d.createFolderTree(targetParent, folders)
			if err != nil {
				return err
			}
		}

		targetFile := targetFolder.File(sourceFilename)
		if targetFile != nil {
			return os.ErrExist
		}

		targetFile, err := targetFolder.NewFile(targetFilename)
		if err != nil {
			return err
		}
		targetFile.Reset(sourceFile.Mime, sourceFile.Size)
		targetFile.Locked = false
		sourceFile.CloneInto(targetFile)

		return d.cluster.CreateShadow(targetFile.Chunks)
	})

	if err != nil {
		if overwrite && err == os.ErrExist {
			if err := d.deleteFile(target); err != nil {
				return err
			}
			return d.copyFile(source, target, overwrite)
		}
		return err
	}
	return nil
}

func (d *dfs) createFolderTree(folderPath string, folders map[string]*common.Folder) (*common.Folder, error) {
	folderTree := common.PathTree(folderPath)

	folder := folders[folderPath]
	if folder == nil {
		var parentFolder *common.Folder
		for _, k := range folderTree {
			if folders[k] != nil {
				parentFolder = folders[k]
				continue
			}

			if parentFolder == nil {
				folders[k] = common.NewFolder(k)
				parentFolder = folders[k]
				continue
			}

			_, name := common.Split(k)
			if err := parentFolder.NewFolder(name, func(folderShadow *common.FolderShadow) error {
				folders[k] = common.NewFolder(folderShadow.Full)
				return nil
			}); err != nil {
				return nil, err
			}
			parentFolder = folders[k]
		}
		folder = folders[folderPath]
	}

	return folder, nil
}

func (d *dfs) Delete(target string) error {
	if err := d.deleteFolder(target); err != nil {
		if err != os.ErrNotExist {
			return err
		}
		return d.deleteFile(target)
	}
	return nil
}

func (d *dfs) deleteFolder(folderPath string) error {
	parentPath, childPath := common.Split(folderPath)

	return d.metadata.Save([]string{parentPath}, func(folders map[string]*common.Folder) error {
		folder := folders[parentPath]
		if folder == nil {
			return os.ErrNotExist
		}

		return folder.DeleteFolder(childPath, func(fullPath string) error {
			return d.metadata.Lock([]string{fullPath}, func(deletingFolders []*common.Folder) error {
				deletingFolder := deletingFolders[0]

				if deletingFolder.Locked() {
					return errors.ErrLock
				}

				for len(deletingFolder.Files) > 0 {
					file := deletingFolder.Files[0]

					if err := deletingFolder.DeleteFile(file.Name, func(file *common.File) error {
						return d.cluster.Delete(file.Chunks)
					}); err != nil {
						return err
					}
				}
				folders[deletingFolder.Full] = nil

				return d.metadata.LockChildrenOf(deletingFolder.Full, func(subFolders []*common.Folder) error {
					for _, folder := range subFolders {
						if folder.Locked() {
							return errors.ErrLock
						}

						for len(folder.Files) > 0 {
							file := folder.Files[0]

							if err := folder.DeleteFile(file.Name, func(file *common.File) error {
								return d.cluster.Delete(file.Chunks)
							}); err != nil {
								return err
							}
						}
						folders[folder.Full] = nil
					}
					return nil
				})
			})
		})
	})
}

func (d *dfs) deleteFile(path string) error {
	folderPath, filename := common.Split(path)

	return d.metadata.Save([]string{folderPath}, func(folders map[string]*common.Folder) error {
		folder := folders[folderPath]
		if folder == nil {
			return os.ErrNotExist
		}

		return folder.DeleteFile(filename, func(file *common.File) error {
			if file.Locked {
				return errors.ErrLock
			}
			return d.cluster.Delete(file.Chunks)
		})
	})
}

func (d *dfs) update(folderPath string, file *common.File) error {
	parent, filename := common.Split(folderPath)

	return d.metadata.Save([]string{parent}, func(folders map[string]*common.Folder) error {
		folder := folders[parent]
		if folder == nil {
			return os.ErrNotExist
		}
		folder.ReplaceFile(filename, file)
		return nil
	})
}

var _ Dfs = &dfs{}
