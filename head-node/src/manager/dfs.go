package manager

import (
	"io"
	"os"
	"strings"

	"github.com/freakmaxi/kertish-dfs/basics/src/common"
	"github.com/freakmaxi/kertish-dfs/basics/src/errors"
	"github.com/freakmaxi/kertish-dfs/head-node/src/data"
)

type Dfs interface {
	CreateFolder(folderPath string) error
	CreateFile(path string, mime string, size uint64, contentReader io.Reader, overwrite bool) error

	Read(paths []string, join bool,
		folderHandler func(folder *common.Folder) error,
		fileHandler func(file *common.File, streamHandler func(writer io.Writer, begins int64, ends int64) error) error) error
	Size(folderPath string) (uint64, error)

	Move(sources []string, target string, join bool, overwrite bool) error
	Copy(sources []string, target string, join bool, overwrite bool) error
	Delete(path string, killZombies bool) error
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
		deletedChunkHashes, missingChunkHashes, err := d.cluster.Delete(file.Chunks)
		file.Ingest(deletedChunkHashes, missingChunkHashes)

		if err != nil {
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

func (d *dfs) Read(paths []string, join bool,
	folderHandler func(folder *common.Folder) error,
	fileHandler func(file *common.File, streamHandler func(writer io.Writer, begins int64, ends int64) error) error) error {

	if len(paths) == 1 {
		if join {
			return os.ErrInvalid
		}
		if err := d.folder(paths[0], folderHandler); err != nil {
			if err != os.ErrNotExist {
				return err
			}
			return d.file(paths, fileHandler)
		}
		return nil
	}
	if !join {
		return os.ErrInvalid
	}
	return d.file(paths, fileHandler)
}

func (d *dfs) Size(folderPath string) (uint64, error) {
	folderPath = common.CorrectPath(folderPath)
	size := uint64(0)

	if err := d.metadata.LockTree(folderPath, true, false, func(folders []*common.Folder) error {
		for _, folder := range folders {
			for _, file := range folder.Files {
				if file.Locked {
					continue
				}
				size += file.Size
			}
		}
		return nil
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

func (d *dfs) file(paths []string, fileHandler func(file *common.File, streamHandler func(writer io.Writer, begins int64, ends int64) error) error) error {
	files := make(common.Files, 0)
	for _, path := range paths {
		folderPath, filename := common.Split(path)
		if len(filename) == 0 {
			return os.ErrInvalid
		}

		if err := d.metadata.Lock([]string{folderPath}, func(folders []*common.Folder) error {
			file := folders[0].File(filename)
			if file == nil {
				return os.ErrNotExist
			}
			files = append(files, file)
			return nil
		}); err != nil {
			return err
		}
	}

	if len(files) == 1 {
		return fileHandler(files[0], func(writer io.Writer, begins int64, ends int64) error {
			return d.cluster.Read(files[0].Chunks, writer, begins, ends)
		})
	}

	joinedFile, err := common.CreateJoinedFile(files)
	if err != nil {
		return err
	}

	return fileHandler(joinedFile, func(writer io.Writer, begins int64, ends int64) error {
		return d.cluster.Read(joinedFile.Chunks, writer, begins, ends)
	})
}

func (d *dfs) Move(sources []string, target string, join bool, overwrite bool) error {
	if len(sources) > 1 && !join {
		return os.ErrInvalid
	}
	if err := d.moveFolder(sources, target, overwrite); err != nil {
		if err != os.ErrNotExist {
			return err
		}
		return d.moveFile(sources, target, overwrite)
	}
	return nil
}

func (d *dfs) moveFolder(sources []string, target string, overwrite bool) error {
	sources = common.CorrectPaths(sources)
	target = common.CorrectPath(target)

	folderPaths := make([]string, 0)
	for _, source := range sources {
		sourceParent, _ := common.Split(source)
		folderPaths = append(folderPaths, sourceParent)
	}
	folderPaths = append(folderPaths, sources...)
	folderPaths = append(folderPaths, common.PathTree(target)...)

	err := d.metadata.Save(folderPaths, func(folders map[string]*common.Folder) error {
		sourceFolders := make([]*common.Folder, 0)
		for _, source := range sources {
			sourceFolder := folders[source]
			if sourceFolder == nil {
				return os.ErrNotExist
			}
			if sourceFolder.Locked() {
				return errors.ErrLock
			}
			sourceFolders = append(sourceFolders, sourceFolder)
		}

		joinedFolder, err := common.CreateJoinedFolder(sourceFolders)
		if err != nil {
			return err
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

		joinedFolder.CloneInto(targetFolder)

		for _, source := range sources {
			sourceParent, sourceChild := common.Split(source)

			if err := folders[sourceParent].DeleteFolder(sourceChild, func(fullPath string) error {
				folders[fullPath] = nil

				return d.metadata.LockTree(fullPath, false, false, func(sourceChildren []*common.Folder) error {
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
		}

		return nil
	})

	if err != nil {
		if overwrite && err == os.ErrExist {
			if err := d.deleteFolder(target, false); err != nil {
				return err
			}
			return d.moveFolder(sources, target, overwrite)
		}
		return err
	}
	return nil
}

func (d *dfs) moveFile(sources []string, target string, overwrite bool) error {
	targetParent, targetFilename := common.Split(target)

	folderPaths := make([]string, 0)
	for _, source := range sources {
		sourceParent, _ := common.Split(source)
		folderPaths = append(folderPaths, sourceParent)
	}
	folderPaths = append(folderPaths, common.PathTree(targetParent)...)

	err := d.metadata.Save(folderPaths, func(folders map[string]*common.Folder) error {
		sourceFiles := make(common.Files, 0)
		for _, source := range sources {
			sourceParent, sourceFilename := common.Split(source)

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

			sourceFiles = append(sourceFiles, sourceFile)
		}

		sourceFile, err := common.CreateJoinedFile(sourceFiles)
		if err != nil {
			return err
		}
		sourceFile.Locked = false

		targetFolder := folders[target]
		if targetFolder == nil {
			var err error
			targetFolder, err = d.createFolderTree(targetParent, folders)
			if err != nil {
				return err
			}
		}

		targetFile, err := targetFolder.NewFile(targetFilename)
		if err != nil {
			return err
		}
		targetFile.Reset(sourceFile.Mime, sourceFile.Size)
		sourceFile.CloneInto(targetFile)

		for _, source := range sources {
			sourceParent, sourceFilename := common.Split(source)
			sourceFolder := folders[sourceParent]

			if err := sourceFolder.DeleteFile(sourceFilename, func(file *common.File) error {
				return nil
			}); err != nil {
				return err
			}
		}

		return nil
	})

	if err != nil {
		if overwrite && err == os.ErrExist {
			if err := d.deleteFile(target, false); err != nil {
				return err
			}
			return d.moveFile(sources, target, overwrite)
		}
		return err
	}
	return nil
}

func (d *dfs) Copy(sources []string, target string, join bool, overwrite bool) error {
	if len(sources) > 1 && !join {
		return os.ErrInvalid
	}
	if err := d.copyFolder(sources, target, overwrite); err != nil {
		if err != os.ErrNotExist {
			return err
		}
		return d.copyFile(sources, target, overwrite)
	}
	return nil
}

func (d *dfs) copyFolder(sources []string, target string, overwrite bool) error {
	sources = common.CorrectPaths(sources)
	target = common.CorrectPath(target)

	folderPaths := make([]string, 0)
	folderPaths = append(folderPaths, sources...)
	folderPaths = append(folderPaths, common.PathTree(target)...)

	err := d.metadata.Save(folderPaths, func(folders map[string]*common.Folder) error {
		sourceFolders := make([]*common.Folder, 0)
		for _, source := range sources {
			sourceFolder := folders[source]
			if sourceFolder == nil {
				return os.ErrNotExist
			}
			sourceFolders = append(sourceFolders, sourceFolder)
		}

		joinedFolder, err := common.CreateJoinedFolder(sourceFolders)
		if err != nil {
			return err
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

		joinedFolder.CloneInto(targetFolder)

		for _, file := range targetFolder.Files {
			if file.Locked {
				continue
			}
			if err := d.cluster.CreateShadow(file.Chunks); err != nil {
				return err
			}
		}

		for _, sourceFolder := range sourceFolders {
			if err := d.metadata.LockTree(sourceFolder.Full, false, false, func(sourceChildren []*common.Folder) error {
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
			}); err != nil {
				return err
			}
		}

		return nil
	})

	if err != nil {
		if overwrite && err == os.ErrExist {
			if err := d.deleteFolder(target, false); err != nil {
				return err
			}
			return d.copyFolder(sources, target, overwrite)
		}
		return err
	}
	return nil
}

func (d *dfs) copyFile(sources []string, target string, overwrite bool) error {
	targetParent, targetFilename := common.Split(target)

	folderPaths := make([]string, 0)
	for _, source := range sources {
		sourceParent, _ := common.Split(source)
		folderPaths = append(folderPaths, sourceParent)
	}
	folderPaths = append(folderPaths, common.PathTree(targetParent)...)

	err := d.metadata.Save(folderPaths, func(folders map[string]*common.Folder) error {
		sourceFiles := make(common.Files, 0)
		for _, source := range sources {
			sourceParent, sourceFilename := common.Split(source)

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

			sourceFiles = append(sourceFiles, sourceFile)
		}

		sourceFile, err := common.CreateJoinedFile(sourceFiles)
		if err != nil {
			return err
		}
		sourceFile.Locked = false

		targetFolder := folders[target]
		if targetFolder == nil {
			var err error
			targetFolder, err = d.createFolderTree(targetParent, folders)
			if err != nil {
				return err
			}
		}

		targetFile, err := targetFolder.NewFile(targetFilename)
		if err != nil {
			return err
		}
		targetFile.Reset(sourceFile.Mime, sourceFile.Size)
		sourceFile.CloneInto(targetFile)

		return d.cluster.CreateShadow(targetFile.Chunks)
	})

	if err != nil {
		if overwrite && err == os.ErrExist {
			if err := d.deleteFile(target, false); err != nil {
				return err
			}
			return d.copyFile(sources, target, overwrite)
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
	parentPath, childPath := common.Split(folderPath)

	return d.metadata.Save([]string{parentPath}, func(folders map[string]*common.Folder) error {
		folder := folders[parentPath]
		if folder == nil {
			return os.ErrNotExist
		}

		return folder.DeleteFolder(childPath, func(fullPath string) error {
			return d.metadata.LockTree(fullPath, true, true, func(deletingFolders []*common.Folder) error {
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
							deletedChunkHashes, missingChunkHashes, err := d.cluster.Delete(file.Chunks)
							file.Ingest(deletedChunkHashes, missingChunkHashes)

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
						folders[changedFolder.Full] = changedFolder
					}

					folders[folder.Full] = nil
				}
				return nil
			})
		})
	})
}

func (d *dfs) deleteFile(path string, killZombies bool) error {
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

			deletedChunkHashes, missingChunkHashes, err := d.cluster.Delete(file.Chunks)
			file.Ingest(deletedChunkHashes, missingChunkHashes)

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
