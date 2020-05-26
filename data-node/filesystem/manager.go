package filesystem

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"

	"github.com/freakmaxi/kertish-dfs/basics/common"
	"github.com/freakmaxi/kertish-dfs/basics/errors"
	"github.com/freakmaxi/kertish-dfs/data-node/cluster"
)

type Manager interface {
	LockFile(sha512Hex string, fileHandler func(blockFile BlockFile) error) error
	File(sha512Hex string, fileHandler func(blockFile BlockFile) error) error

	List() (common.SyncFileItems, error)
	Sync(sourceAddr string) error
	Wipe() error
	NodeSize() uint64
	Used() (uint64, error)
}

type manager struct {
	rootPath string
	nodeSize uint64

	syncLock         sync.Mutex
	createDeleteLock map[string]*sync.Mutex

	nodeCacheMutex sync.Mutex
	nodeCache      map[string]cluster.DataNode
}

func NewManager(rootPath string, nodeSize uint64) Manager {
	return &manager{
		rootPath:         rootPath,
		nodeSize:         nodeSize,
		syncLock:         sync.Mutex{},
		createDeleteLock: make(map[string]*sync.Mutex),
		nodeCacheMutex:   sync.Mutex{},
		nodeCache:        make(map[string]cluster.DataNode),
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
	m.syncLock.Lock()
	defer m.syncLock.Unlock()

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

func (m *manager) List() (common.SyncFileItems, error) {
	m.syncLock.Lock()
	defer m.syncLock.Unlock()

	return m.list()
}

func (m *manager) list() (common.SyncFileItems, error) {
	if err := m.prepareRoot(); err != nil {
		return nil, err
	}

	sha512HexList := make(common.SyncFileItems, 0)
	if err := filepath.Walk(m.rootPath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() && len(info.Name()) == 64 {
			sha512HexList = append(sha512HexList, common.SyncFileItem{
				Sha512Hex: info.Name(),
				Size:      int32(info.Size() - headerSize),
			})
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

	m.nodeCacheMutex.Lock()
	dn, has := m.nodeCache[sourceAddr]
	m.nodeCacheMutex.Unlock()

	if !has {
		var err error
		dn, err = cluster.NewDataNode(sourceAddr)
		if err != nil {
			return err
		}

		m.nodeCacheMutex.Lock()
		m.nodeCache[sourceAddr] = dn
		m.nodeCacheMutex.Unlock()
	}

	currentFileItemList, err := m.list()
	if err != nil {
		return err
	}
	sort.Sort(currentFileItemList)

	sourceFileItemList := make(common.SyncFileItems, 0)
	if err := dn.SyncList(func(fileItem common.SyncFileItem, current uint64, total uint64) error {
		sourceFileItemList = append(sourceFileItemList, fileItem)
		return nil
	}); err != nil {
		return err
	}
	sort.Sort(sourceFileItemList)

	wipeList := make(common.SyncFileItems, 0)
	createList := make(common.SyncFileItems, 0)

	for len(sourceFileItemList) > 0 {
		sourceFileItem := sourceFileItemList[0]

		if len(currentFileItemList) == 0 {
			createList = append(createList, sourceFileItem)
			sourceFileItemList = sourceFileItemList[1:]
			continue
		}

		currentFileItem := currentFileItemList[0]

		if common.Compare(sourceFileItem, currentFileItem) {
			sourceFileItemList = sourceFileItemList[1:]
			currentFileItemList = currentFileItemList[1:]

			continue
		}

		wipe := false
		for i, currentFileItem := range currentFileItemList[1:] {
			if common.Compare(sourceFileItem, currentFileItem) {
				wipeList = append(wipeList, currentFileItemList[:i+1]...)
				currentFileItemList = currentFileItemList[i+2:]
				wipe = true
				break
			}
		}
		if wipe {
			sourceFileItemList = sourceFileItemList[1:]
			continue
		}

		createList = append(createList, sourceFileItem)
		sourceFileItemList = sourceFileItemList[1:]
	}

	if len(currentFileItemList) > 0 {
		wipeList = append(wipeList, currentFileItemList...)
	}

	fmt.Printf("INFO: Sync will, create: %d / delete: %d\n", len(createList), len(wipeList))

	createHandler := func(sha512Hex string, current int, total int) (resultError error) {
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

			return dn.SyncRead(
				sha512Hex,
				false,
				func(blockSize uint32, usageCount uint16) bool {
					usageCountBackup = usageCount

					if blockFile.Temporary() {
						return true
					}

					size, _ := blockFile.Size()
					if size != blockSize {
						return true
					}

					if !blockFile.VerifyForce() {
						if blockFile.Truncate(blockSize) != nil {
							fmt.Print(" FAILED!")
							return false
						}
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
						verifyResult := blockFile.Verify()
						if verifyResult {
							fmt.Print(" done!")
						}
						return verifyResult
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

	wg := &sync.WaitGroup{}

	wg.Add(1)
	go func(wg *sync.WaitGroup) {
		defer wg.Done()

		totalWipeCount := len(wipeList)
		for len(wipeList) > 0 {
			if err := deleteHandler(wipeList[0].Sha512Hex); err != nil {
				fmt.Printf("ERROR: Sync cannot delete (%s) - %d / %d: %s\n", wipeList[0].Sha512Hex, totalWipeCount-(len(wipeList)-1), totalWipeCount, err.Error())
				continue
			}
			wipeList = wipeList[1:]
		}
	}(wg)

	wg.Add(1)
	go func(wg *sync.WaitGroup) {
		defer wg.Done()

		totalCreateCount := len(createList)
		for len(createList) > 0 {
			if err := createHandler(createList[0].Sha512Hex, totalCreateCount-(len(createList)-1), totalCreateCount); err != nil {
				fmt.Printf("ERROR: Sync cannot create (%s) - %d / %d: %s\n", createList[0].Sha512Hex, totalCreateCount-(len(createList)-1), totalCreateCount, err.Error())
				continue
			}
			createList = createList[1:]
		}
	}(wg)

	wg.Wait()

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

	currentFileItemList, err := m.list()
	if err != nil {
		return err
	}

	for len(currentFileItemList) > 0 {
		if err := deleteHandler(currentFileItemList[0].Sha512Hex); err != nil {
			continue
		}
		currentFileItemList = currentFileItemList[1:]
	}

	return nil
}

func (m *manager) NodeSize() uint64 {
	return m.nodeSize
}

func (m *manager) Used() (uint64, error) {
	if err := m.prepareRoot(); err != nil {
		return 0, err
	}

	used := uint64(0)
	if err := filepath.Walk(m.rootPath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() && len(info.Name()) == 64 {
			if size := info.Size(); size > 0 {
				used += uint64(size)
			}
		}
		return nil
	}); err != nil {
		return 0, err
	}

	return used, nil
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
