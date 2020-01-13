package filesystem

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"

	"github.com/freakmaxi/2020-dfs/data-node/src/cluster"
	"github.com/freakmaxi/2020-dfs/data-node/src/errors"
)

type Manager interface {
	LockFile(sha512Hex string, fileHandler func(blockFile BlockFile) error) error
	File(sha512Hex string, fileHandler func(blockFile BlockFile) error) error

	List() ([]string, error)
	Sync(sourceAddr string) error
	Wipe() error
	NodeSize() uint64
}

type manager struct {
	rootPath string
	nodeSize uint64

	syncLock         *sync.Mutex
	createDeleteLock map[string]*sync.Mutex
}

func NewManager(rootPath string, nodeSize uint64) Manager {
	return &manager{
		rootPath:         rootPath,
		nodeSize:         nodeSize,
		syncLock:         &sync.Mutex{},
		createDeleteLock: make(map[string]*sync.Mutex),
	}
}

func (m *manager) lock(sha512Hex string) {
	m.syncLock.Lock()
	l, has := m.createDeleteLock[sha512Hex]
	if !has {
		l = &sync.Mutex{}
		m.createDeleteLock[sha512Hex] = l
	}
	m.syncLock.Unlock()

	l.Lock()
}

func (m *manager) unLock(sha512Hex string) {
	m.createDeleteLock[sha512Hex].Unlock()
}

func (m *manager) LockFile(sha512Hex string, fileHandler func(blockFile BlockFile) error) error {
	m.lock(sha512Hex)
	defer m.unLock(sha512Hex)

	return m.File(sha512Hex, fileHandler)
}

func (m *manager) File(sha512Hex string, fileHandler func(blockFile BlockFile) error) error {
	if err := m.prepareRoot(); err != nil {
		return err
	}

	blockFile, err := NewBlockFile(m.rootPath, sha512Hex)
	if err != nil {
		return err
	}
	defer blockFile.Close()

	return fileHandler(blockFile)
}

func (m *manager) List() ([]string, error) {
	m.syncLock.Lock()
	defer m.syncLock.Unlock()

	return m.list()
}

func (m *manager) list() ([]string, error) {
	if err := m.prepareRoot(); err != nil {
		return nil, err
	}

	sha512HexList := make([]string, 0)
	if err := filepath.Walk(m.rootPath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() && len(info.Name()) == 64 {
			sha512HexList = append(sha512HexList, info.Name())
		}
		return nil
	}); err != nil {
		return nil, err
	}

	return sha512HexList, nil
}

// Full Sync with Master Data Node
func (m *manager) Sync(sourceAddr string) error {
	m.syncLock.Lock()
	defer m.syncLock.Unlock()

	createHandler := func(sha512Hex string, current uint64, total uint64) (resultError error) {
		fmt.Printf("INFO: Syncing (%s) - %d / %d...", sha512Hex, current, total)
		defer func() {
			if resultError == nil || resultError == errors.ErrQuit {
				fmt.Print("\n")
			} else {
				fmt.Printf(" FAILED! (%s)\n", resultError.Error())
			}
		}()

		return m.File(sha512Hex, func(blockFile BlockFile) error {
			usageCountBackup := uint16(1)

			return cluster.NewDataNode(sourceAddr).SyncRead(
				sha512Hex,
				func(usageCount uint16) bool {
					usageCountBackup = usageCount

					if blockFile.Temporary() {
						return true
					}

					if blockFile.ResetUsage(usageCount) != nil {
						return true
					}

					fmt.Print(" already synced!")
					return false
				},
				func(data []byte) error {
					return blockFile.Write(data)
				},
				func() bool {
					if usageCountBackup == 1 {
						return blockFile.Verify()
					}

					if err := blockFile.ResetUsage(usageCountBackup); err != nil {
						return false
					}

					if !blockFile.Verify() {
						return false
					}

					fmt.Print(" done!")
					return true
				})
		})
	}
	deleteHandler := func(sha512Hex string) error {
		return m.File(sha512Hex, func(blockFile BlockFile) error {
			return blockFile.Wipe()
		})
	}

	currentSha512HexList, err := m.list()
	if err != nil {
		return err
	}
	sort.Strings(currentSha512HexList)

	sourceSha512HexList := make([]string, 0)
	if err := cluster.NewDataNode(sourceAddr).SyncList(func(sha512Hex string, current uint64, total uint64) error {
		sourceSha512HexList = append(sourceSha512HexList, sha512Hex)
		return nil
	}); err != nil {
		return err
	}
	sort.Strings(sourceSha512HexList)

	totalNum := uint64(len(sourceSha512HexList))

	for len(sourceSha512HexList) > 0 {
		sha512Hex := sourceSha512HexList[0]
		if len(currentSha512HexList) == 0 {
			if err := createHandler(sha512Hex, (totalNum-uint64(len(sourceSha512HexList)))+1, totalNum); err != nil {
				continue
			}
			sourceSha512HexList = sourceSha512HexList[1:]
			continue
		}
		currentSha512Hex := currentSha512HexList[0]

		if strings.Compare(sha512Hex, currentSha512Hex) == 0 {
			sourceSha512HexList = sourceSha512HexList[1:]
			currentSha512HexList = currentSha512HexList[1:]

			continue
		}

		var wipeList []string
		for i, sSha512Hex := range currentSha512HexList[1:] {
			if strings.Compare(sha512Hex, sSha512Hex) != 0 {
				continue
			}

			wipeList = currentSha512HexList[:i]

			sourceSha512HexList = sourceSha512HexList[1:]
			currentSha512HexList = currentSha512HexList[i+1:]

			break
		}

		if len(wipeList) > 0 {
			for len(wipeList) > 0 {
				if err := deleteHandler(wipeList[0]); err != nil {
					continue
				}
				wipeList = wipeList[1:]
			}
		} else {
			if err := createHandler(sha512Hex, (totalNum-uint64(len(sourceSha512HexList)))+1, totalNum); err != nil {
				continue
			}
			sourceSha512HexList = sourceSha512HexList[1:]
		}
	}

	for len(currentSha512HexList) > 0 {
		if err := deleteHandler(currentSha512HexList[0]); err != nil {
			continue
		}
		currentSha512HexList = currentSha512HexList[1:]
	}

	return nil
}

func (m *manager) Wipe() error {
	m.syncLock.Lock()
	defer m.syncLock.Unlock()

	deleteHandler := func(sha512Hex string) error {
		return m.File(sha512Hex, func(blockFile BlockFile) error {
			return blockFile.Wipe()
		})
	}

	currentSha512HexList, err := m.list()
	if err != nil {
		return err
	}

	for len(currentSha512HexList) > 0 {
		if err := deleteHandler(currentSha512HexList[0]); err != nil {
			continue
		}
		currentSha512HexList = currentSha512HexList[1:]
	}

	return nil
}

func (m *manager) NodeSize() uint64 {
	return m.nodeSize
}

func (m *manager) prepareRoot() error {
	_, err := os.Stat(m.rootPath)
	if err != nil {
		if err == os.ErrNotExist {
			return os.Mkdir(m.rootPath, 0666)
		}
		return err
	}
	return nil
}

var _ Manager = &manager{}
