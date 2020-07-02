package manager

import (
	"io"

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

var _ Dfs = &dfs{}
