package filesystem

import (
	"io/ioutil"
	"os"
	"path"
	"sync"

	dnc "github.com/freakmaxi/kertish-dfs/data-node/common"
	"github.com/freakmaxi/kertish-dfs/data-node/filesystem/block"
	"go.uber.org/zap"
)

// Manager interface is for handling data node operations
type Manager interface {
	Block() block.Manager
	Snapshot(func(snapshot Snapshot) error) error
	Sync(func(sync Synchronize) error) error

	Wipe() error
	Used() (uint64, error)
}

type manager struct {
	rootPath string
	logger   *zap.Logger

	block       block.Manager
	snapshot    Snapshot
	synchronize Synchronize

	mutex sync.Mutex
}

// NewManager creates the instance of data node operations manager
func NewManager(rootPath string, logger *zap.Logger) (Manager, error) {
	b, err := block.NewManager(rootPath, logger)
	if err != nil {
		return nil, err
	}

	ss := NewSnapshot(rootPath, logger)
	s, err := NewSynchronize(rootPath, ss, logger)
	if err != nil {
		return nil, err
	}

	return &manager{
		rootPath:    rootPath,
		logger:      logger,
		block:       b,
		snapshot:    ss,
		synchronize: s,
		mutex:       sync.Mutex{},
	}, nil
}

func (m *manager) Block() block.Manager {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	return m.block
}

func (m *manager) Snapshot(handler func(snapshot Snapshot) error) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	m.block.Wait()

	return handler(m.snapshot)
}

func (m *manager) Sync(handler func(sync Synchronize) error) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	m.block.Wait()

	return handler(m.synchronize)
}

func (m *manager) Wipe() error {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	m.block.Wait()

	infos, err := ioutil.ReadDir(m.rootPath)
	if err != nil {
		return err
	}

	for _, info := range infos {
		p := path.Join(m.rootPath, info.Name())

		if info.IsDir() {
			if err := os.RemoveAll(p); err != nil {
				return err
			}
			continue
		}

		if err := os.Remove(p); err != nil {
			return err
		}
	}

	return nil
}

func (m *manager) Used() (uint64, error) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	m.block.Wait()

	sha512HexMap := make(map[string]uint64)

	// fill for root
	if err := dnc.Traverse(m.rootPath, func(info os.FileInfo) error {
		sha512HexMap[info.Name()] = uint64(info.Size())
		return nil
	}); err != nil {
		return 0, err
	}

	// fill for snapshotDates
	snapshotDates, err := m.snapshot.Dates()
	if err != nil {
		return 0, err
	}

	for _, snapshotDate := range snapshotDates {
		snapshotPathName := m.snapshot.PathName(snapshotDate)
		if err := dnc.Traverse(path.Join(m.rootPath, snapshotPathName), func(info os.FileInfo) error {
			sha512HexMap[info.Name()] = uint64(info.Size())
			return nil
		}); err != nil {
			return 0, err
		}
	}

	used := uint64(0)
	for _, v := range sha512HexMap {
		used += v
	}

	return used, nil
}

var _ Manager = &manager{}
