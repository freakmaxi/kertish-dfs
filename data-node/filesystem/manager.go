package filesystem

import (
	"os"
	"path"
	"sync"

	"github.com/freakmaxi/kertish-dfs/data-node/filesystem/block"
	"go.uber.org/zap"
)

type BlockRequestType int

const (
	Read   BlockRequestType = 1
	Create BlockRequestType = 2
	Delete BlockRequestType = 3
)

// Manager interface is for handling data node operations
type Manager interface {
	Block(requestType BlockRequestType) block.Manager
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

	managerMutex sync.Mutex
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
		rootPath:     rootPath,
		logger:       logger,
		block:        b,
		snapshot:     ss,
		synchronize:  s,
		managerMutex: sync.Mutex{},
	}, nil
}

func (m *manager) wait() {
	m.managerMutex.Lock()
	defer m.managerMutex.Unlock()
}

func (m *manager) Block(requestType BlockRequestType) block.Manager {
	switch requestType {
	case Create, Delete:
		// Wait if there is any snapshot or wipe operation
		m.wait()
	case Read:
		// No need to lock for read requests in manager level
		// File level lock will be enough to handle multiple operations
	}

	return m.block
}

func (m *manager) Snapshot(handler func(snapshot Snapshot) error) error {
	m.managerMutex.Lock()
	defer m.managerMutex.Unlock()

	return handler(m.snapshot)
}

func (m *manager) Sync(handler func(sync Synchronize) error) error {
	// Wait if there is any snapshot or wipe operation
	m.wait()

	return handler(m.synchronize)
}

func (m *manager) Wipe() error {
	m.managerMutex.Lock()
	defer m.managerMutex.Unlock()

	if err := m.block.Traverse(func(sha512Hex string, size uint64) error {
		p := path.Join(m.rootPath, sha512Hex)
		return os.Remove(p)
	}); err != nil {
		return err
	}

	snapshotDates, err := m.snapshot.Dates()
	if err != nil {
		return err
	}

	for _, snapshotDate := range snapshotDates {
		if err := m.snapshot.Delete(snapshotDate); err != nil {
			return err
		}
	}

	return nil
}

func (m *manager) Used() (uint64, error) {
	sha512HexMap := make(map[string]uint64)

	// fill for root
	if err := m.block.Traverse(func(sha512Hex string, size uint64) error {
		sha512HexMap[sha512Hex] = size
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
		snapshotBlock, err := m.snapshot.Block(snapshotDate)
		if err != nil {
			return 0, err
		}

		if err := snapshotBlock.Traverse(func(sha512Hex string, size uint64) error {
			sha512HexMap[sha512Hex] = size
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
