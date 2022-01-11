package block

import (
	"os"
	"sync"

	"github.com/freakmaxi/kertish-dfs/data-node/common"
	"go.uber.org/zap"
)

// Manager interface is for file operation handling
type Manager interface {
	Wait()

	File(sha512Hex string, fileHandler func(file File) error) error
	LockFile(sha512Hex string, fileHandler func(file File) error) error

	Traverse(hexHandler func(sha512Hex string) error) error

	Wipe() error
}

type manager struct {
	dataPath string
	logger   *zap.Logger

	blockLockMutex sync.Mutex
	blockLock      map[string]*sync.Mutex
}

// NewManager creates the Manager interface for file operation handling
func NewManager(dataPath string, logger *zap.Logger) (Manager, error) {
	m := &manager{
		dataPath: dataPath,
		logger:   logger,

		blockLockMutex: sync.Mutex{},
		blockLock:      make(map[string]*sync.Mutex),
	}

	if err := m.prepare(); err != nil {
		return nil, err
	}

	return m, nil
}

func (m *manager) prepare() error {
	_, err := os.Stat(m.dataPath)
	if err != nil {
		if os.IsNotExist(err) {
			return os.MkdirAll(m.dataPath, 0777)
		}
		return err
	}
	return nil
}

func (m *manager) lock(sha512Hex string) {
	m.blockLockMutex.Lock()
	l, has := m.blockLock[sha512Hex]
	if !has {
		l = &sync.Mutex{}
		m.blockLock[sha512Hex] = l
	}
	m.blockLockMutex.Unlock()

	l.Lock()
}

func (m *manager) unlock(sha512Hex string) {
	m.blockLockMutex.Lock()
	defer m.blockLockMutex.Unlock()

	m.blockLock[sha512Hex].Unlock()
}

func (m *manager) Wait() {
	m.blockLockMutex.Lock()
	defer m.blockLockMutex.Unlock()
}

func (m *manager) File(sha512Hex string, fileHandler func(file File) error) error {
	file, err := NewFile(m.dataPath, sha512Hex, m.logger)
	if err != nil {
		return err
	}
	defer file.Close()

	return fileHandler(file)
}

func (m *manager) LockFile(sha512Hex string, fileHandler func(file File) error) error {
	m.lock(sha512Hex)
	defer m.unlock(sha512Hex)

	return m.File(sha512Hex, fileHandler)
}

func (m *manager) Traverse(hexHandler func(sha512Hex string) error) error {
	return common.Traverse(m.dataPath, func(info os.FileInfo) error {
		return hexHandler(info.Name())
	})
}

func (m *manager) compileSha512HexList() ([]string, error) {
	m.blockLockMutex.Lock()
	defer m.blockLockMutex.Unlock()

	sha512HexList := make([]string, 0)

	if err := common.Traverse(m.dataPath, func(info os.FileInfo) error {
		sha512HexList = append(sha512HexList, info.Name())
		return nil
	}); err != nil {
		return nil, err
	}

	return sha512HexList, nil
}

func (m *manager) Wipe() error {
	sha512HexList, err := m.compileSha512HexList()
	if err != nil {
		return err
	}

	for len(sha512HexList) > 0 {
		if err := m.LockFile(sha512HexList[0], func(file File) error {
			if file.Temporary() {
				return nil
			}
			return file.Wipe()
		}); err != nil {
			sha512HexList = append(sha512HexList, sha512HexList[0])
		}
		sha512HexList = sha512HexList[1:]
	}

	return nil
}

var _ Manager = &manager{}
