package manager

import (
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/freakmaxi/kertish-dfs/basics/common"
	"github.com/freakmaxi/kertish-dfs/basics/errors"
	"github.com/freakmaxi/kertish-dfs/head-node/data"
	"go.uber.org/zap"
)

type Dfs interface {
	CreateFolder(folderPath string) error
	CreateFile(path string, mime string, size uint64, overwrite bool, contentReader io.Reader) error

	Read(paths []string, join bool) (Read, error)
	Size(folderPath string) (uint64, error)

	Move(sources []string, target string, join bool, overwrite bool) error
	Copy(sources []string, target string, join bool, overwrite bool) error
	Delete(path string, killZombies bool) error
}

type dfs struct {
	metadata data.Metadata
	cluster  Cluster
	logger   *zap.Logger
}

func NewDfs(metadata data.Metadata, cluster Cluster, logger *zap.Logger) Dfs {
	return &dfs{
		metadata: metadata,
		cluster:  cluster,
		logger:   logger,
	}
}

func (d *dfs) CreateFolder(folderPath string) error {
	folderPath = common.CorrectPath(folderPath)
	folderTree := common.PathTree(folderPath)

	matchFolderTree, err := d.metadata.MatchTree(folderTree)
	if err != nil {
		return err
	}
	folderTree = d.filterFolderTree(matchFolderTree, folderTree)

	return d.metadata.Save(folderTree, func(folders map[string]*common.Folder) error {
		folder := folders[folderPath]
		if folder != nil {
			return os.ErrExist
		}

		if _, err := d.createFolderTree(folderTree[0], folderPath, folders); err != nil {
			return err
		}
		return nil
	})
}

// this func is modified to prevent the locking whole folder path tree.
func (d *dfs) CreateFile(path string, mime string, size uint64, overwrite bool, contentReader io.Reader) error {
	path = common.CorrectPath(path) // It is required in here

	folderPath, filename := common.Split(path)
	if len(filename) == 0 {
		return os.ErrInvalid
	}
	folderTree := common.PathTree(folderPath)

	matchFolderTree, err := d.metadata.MatchTree(folderTree)
	if err != nil {
		return err
	}
	folderTree = d.filterFolderTree(matchFolderTree, folderTree)

	var folder *common.Folder
	var file *common.File

	if err := d.metadata.Save(folderTree, func(folders map[string]*common.Folder) error {
		var err error

		folder = folders[folderPath]
		if folder == nil {
			folder, err = d.createFolderTree(folderTree[0], folderPath, folders)
			if err != nil {
				return err
			}
		}

		file = folder.File(filename)
		if file == nil {
			file, err = folder.NewFile(filename)
			return err
		}

		if !overwrite {
			return os.ErrExist
		}

		if file.Locked() {
			return errors.ErrLock
		}
		file.Lock = common.NewFileLockForSize(size)

		return nil
	}); err != nil {
		return err
	}

	if overwrite {
		deletionResult, err := d.cluster.Delete(file.Chunks)
		if err != nil {
			file.Lock.Cancel()

			if errUpdate := d.update(path, file); errUpdate != nil {
				d.logger.Warn(
					fmt.Sprintf(
						"Reverting deletion failure for file creation is failed, file will stay locked until %s",
						file.Lock.Till.Format(common.FriendlyTimeFormat),
					),
					zap.String("path", path),
					zap.Error(errUpdate),
				)
			}

			return err
		}
		file.IngestDeletion(*deletionResult)
	}

	chunks, err := d.cluster.Create(size, contentReader)
	if err != nil {
		if errUpdate := d.update(path, nil); errUpdate != nil {
			d.logger.Error(
				"Dropping file entry due to file creation failure is failed, file is now zombie, repair is required!",
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
			"Saving file creation is failed. File is now zombie with orphan chunks. Repair is required!",
			zap.String("path", path),
			zap.Error(err),
		)
	}
	return err
}

func (d *dfs) Read(paths []string, join bool) (Read, error) {
	if len(paths) == 1 && join || len(paths) > 1 && !join {
		return nil, os.ErrInvalid
	}

	if len(paths) == 1 {
		folder, err := d.folder(paths[0])
		if err == nil {
			return newReadForFolder(folder), nil
		}

		if err != os.ErrNotExist {
			return nil, err
		}
	}

	file, streamHandler, err := d.file(paths)
	if err != nil {
		return nil, err
	}

	return newReadForFile(file, streamHandler), nil
}

func (d *dfs) Size(folderPath string) (uint64, error) {
	folderPath = common.CorrectPath(folderPath)

	folders, err := d.metadata.Tree(folderPath, true, false)
	if err != nil {
		return 0, err
	}

	size := uint64(0)
	for _, folder := range folders {
		for _, file := range folder.Files {
			if file.Locked() {
				continue
			}
			size += file.Size
		}
	}
	return size, nil
}

func (d *dfs) folder(folderPath string) (*common.Folder, error) {
	folderPath = common.CorrectPath(folderPath)

	folders, err := d.metadata.Get([]string{folderPath})
	if err != nil {
		return nil, err
	}
	return folders[0], nil
}

func (d *dfs) file(paths []string) (*common.File, func(w io.Writer, begins int64, ends int64) error, error) {
	files := make(common.Files, 0)
	for _, path := range paths {
		folderPath, filename := common.Split(path)
		if len(filename) == 0 {
			return nil, nil, os.ErrInvalid
		}

		folders, err := d.metadata.Get([]string{folderPath})
		if err != nil {
			return nil, nil, err
		}

		file := folders[0].File(filename)
		if file == nil {
			return nil, nil, os.ErrNotExist
		}

		if file.Locked() {
			return nil, nil, errors.ErrLock
		}

		if file.Zombie {
			return nil, nil, errors.ErrZombie
		}

		files = append(files, file)
	}

	requestedFile := files[0]

	if len(files) > 1 {
		var err error
		requestedFile, err = common.CreateJoinedFile(files)
		if err != nil {
			return nil, nil, err
		}
	}

	streamHandler, err := d.cluster.Read(requestedFile.Chunks)
	if err != nil {
		return nil, nil, err
	}
	return requestedFile, streamHandler, nil
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
			targetFolder, err = d.createFolderTree("/", target, folders)
			if err != nil {
				return err
			}
		}

		joinedFolder.CloneInto(targetFolder)

		for _, source := range sources {
			sourceParent, sourceChild := common.Split(source)

			if err := folders[sourceParent].DeleteFolder(sourceChild, func(fullPath string) error {
				folders[fullPath] = nil

				sourceChildren, err := d.metadata.Tree(fullPath, false, false)
				if err != nil {
					return err
				}

				for _, sourceChild := range sourceChildren {
					if sourceChild.Locked() {
						return errors.ErrLock
					}
					currentFull := sourceChild.Full
					sourceChild.Full = strings.Replace(sourceChild.Full, fullPath, targetFolder.Full, 1)
					folders[currentFull] = sourceChild
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

			if sourceFile.Locked() {
				return errors.ErrLock
			}

			if sourceFile.Zombie {
				return errors.ErrZombie
			}

			sourceFiles = append(sourceFiles, sourceFile)
		}

		sourceFile, err := common.CreateJoinedFile(sourceFiles)
		if err != nil {
			return err
		}

		targetFolder := folders[target]
		if targetFolder == nil {
			var err error
			targetFolder, err = d.createFolderTree("/", targetParent, folders)
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

		targetFile.Lock.Cancel()

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
			targetFolder, err = d.createFolderTree("/", target, folders)
			if err != nil {
				return err
			}
		}

		joinedFolder.CloneInto(targetFolder)

		for _, file := range targetFolder.Files {
			if file.Locked() {
				continue
			}
			if file.Zombie {
				continue
			}
			if err := d.cluster.CreateShadow(file.Chunks); err != nil {
				return err
			}
		}

		for _, sourceFolder := range sourceFolders {
			sourceChildren, err := d.metadata.Tree(sourceFolder.Full, false, false)
			if err != nil {
				return err
			}

			for _, sourceChild := range sourceChildren {
				sourceChild.Full = strings.Replace(sourceChild.Full, sourceFolder.Full, targetFolder.Full, 1)
				for _, file := range sourceChild.Files {
					if file.Locked() {
						continue
					}
					if file.Zombie {
						continue
					}
					if err := d.cluster.CreateShadow(file.Chunks); err != nil {
						return err
					}
				}
				folders[sourceChild.Full] = sourceChild
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

			if sourceFile.Locked() {
				return errors.ErrLock
			}

			if sourceFile.Zombie {
				return errors.ErrZombie
			}

			sourceFiles = append(sourceFiles, sourceFile)
		}

		sourceFile, err := common.CreateJoinedFile(sourceFiles)
		if err != nil {
			return err
		}

		targetFolder := folders[target]
		if targetFolder == nil {
			var err error
			targetFolder, err = d.createFolderTree("/", targetParent, folders)
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
		targetFile.Lock.Cancel()

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

func (d *dfs) filterFolderTree(matches []string, folderTree []string) []string {
	if len(matches) == 0 {
		return folderTree
	}

	for len(folderTree) > 0 {
		if strings.Compare(matches[len(matches)-1], folderTree[0]) == 0 {
			break
		}
		folderTree = folderTree[1:]
	}

	return folderTree
}

func (d *dfs) createFolderTree(base string, folderPath string, folders map[string]*common.Folder) (*common.Folder, error) {
	folderTree := common.PathTree(folderPath)

	folder := folders[folderPath]
	if folder == nil {
		for len(folderTree) > 0 {
			if strings.Compare(base, folderTree[0]) == 0 {
				break
			}
			folderTree = folderTree[1:]
		}

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
						deletionResult, err := d.cluster.Delete(file.Chunks)
						if deletionResult != nil {
							file.IngestDeletion(*deletionResult)
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
}

func (d *dfs) deleteFile(path string, killZombies bool) error {
	folderPath, filename := common.Split(path)

	return d.metadata.Save([]string{folderPath}, func(folders map[string]*common.Folder) error {
		folder := folders[folderPath]
		if folder == nil {
			return os.ErrNotExist
		}

		return folder.DeleteFile(filename, func(file *common.File) error {
			if file.Locked() {
				return errors.ErrLock
			}

			deletionResult, err := d.cluster.Delete(file.Chunks)
			if deletionResult != nil {
				file.IngestDeletion(*deletionResult)
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
