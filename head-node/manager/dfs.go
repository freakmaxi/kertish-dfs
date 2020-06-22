package manager

import (
	"io"
	"strings"

	"github.com/freakmaxi/kertish-dfs/basics/common"
	"github.com/freakmaxi/kertish-dfs/head-node/data"
	"go.uber.org/zap"
)

type Dfs interface {
	CreateFolder(folderPath string) error
	CreateFile(path string, mime string, size uint64, overwrite bool, contentReader io.Reader) error

	Read(paths []string, join bool) (ReadContainer, error)
	Size(folderPath string) (uint64, error)

	Change(sources []string, target string, join bool, overwrite bool, move bool) error

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

		var err error
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

			folders[k], err = parentFolder.NewFolder(name)
			if err != nil {
				return nil, err
			}
			parentFolder = folders[k]
		}
		folder = folders[folderPath]
	}

	return folder, nil
}

var _ Dfs = &dfs{}
