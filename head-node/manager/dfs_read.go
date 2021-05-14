package manager

import (
	"io"
	"os"

	"github.com/freakmaxi/kertish-dfs/basics/common"
	"github.com/freakmaxi/kertish-dfs/basics/errors"
)

func (d *dfs) Read(paths []string, join bool) (ReadContainer, error) {
	if len(paths) == 1 && join || len(paths) > 1 && !join {
		return nil, os.ErrInvalid
	}

	if len(paths) == 1 {
		folder, err := d.folder(paths[0])
		if err == nil {
			return newReadContainerForFolder(folder, d.tree), nil
		}

		if err != os.ErrNotExist {
			return nil, err
		}
	}

	file, streamHandler, err := d.file(paths)
	if err != nil {
		return nil, err
	}

	return newReadContainerForFile(file, streamHandler), nil
}

func (d *dfs) folder(folderPath string) (*common.Folder, error) {
	folderPath = common.CorrectPath(folderPath)

	folders, err := d.metadata.Get([]string{folderPath})
	if err != nil {
		return nil, err
	}
	return folders[0], nil
}

func (d *dfs) tree(folderPath string) (*common.Tree, error) {
	folderPath = common.CorrectPath(folderPath)

	folders, err := d.metadata.ChildrenTree(folderPath, true, false)
	if err != nil {
		return nil, err
	}

	tree := common.NewTree()
	if err := tree.Fill(&folderPath, folders); err != nil {
		return nil, err
	}
	return tree, nil
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

		if file.ZombieCheck() {
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

func (d *dfs) Size(folderPath string) (uint64, error) {
	folderPath = common.CorrectPath(folderPath)

	folders, err := d.metadata.ChildrenTree(folderPath, true, false)
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
