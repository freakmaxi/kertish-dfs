package filesystem

import (
	"fmt"
	"os"
	"path"
	"sync"
	"time"

	"github.com/freakmaxi/kertish-dfs/basics/common"
	"github.com/freakmaxi/kertish-dfs/basics/errors"
	"github.com/freakmaxi/kertish-dfs/data-node/cluster"
	dnc "github.com/freakmaxi/kertish-dfs/data-node/common"
	"github.com/freakmaxi/kertish-dfs/data-node/filesystem/block"
	"go.uber.org/zap"
)

type Synchronize interface {
	Container() (*common.SyncContainer, error)
	Start(sourceAddr string) error
}

type synchronize struct {
	rootPath string
	snapshot Snapshot
	logger   *zap.Logger

	syncMutex sync.Mutex
	nodeCache map[string]cluster.DataNode
}

func NewSynchronize(rootPath string, snapshot Snapshot, logger *zap.Logger) Synchronize {
	return &synchronize{
		rootPath: rootPath,
		snapshot: snapshot,
		logger:   logger,

		syncMutex: sync.Mutex{},
		nodeCache: make(map[string]cluster.DataNode),
	}
}

func (s *synchronize) Container() (*common.SyncContainer, error) {
	s.syncMutex.Lock()
	defer s.syncMutex.Unlock()

	return s.createContainer()
}

func (s *synchronize) createContainer() (*common.SyncContainer, error) {
	var err error

	container := common.NewSyncContainer()
	container.FileItems, err = s.createFileItems(s.rootPath, make(HeaderMap))
	if err != nil {
		return nil, err
	}

	snapshotDates, err := s.snapshot.Dates()
	if err != nil {
		return nil, err
	}

	for _, snapshotDate := range snapshotDates {
		headerMap, err := s.snapshot.ReadHeaderBackup(snapshotDate)
		if err != nil {
			return nil, err
		}

		snapshotPathName := s.snapshot.PathName(snapshotDate)
		snapshotPath := path.Join(s.rootPath, snapshotPathName)

		snapshotContainer := common.NewContainer(snapshotDate)
		snapshotContainer.FileItems, err = s.createFileItems(snapshotPath, headerMap)
		if err != nil {
			return nil, err
		}

		container.Snapshots = append(container.Snapshots, snapshotContainer)
	}
	container.Sort()

	return container, nil
}

func (s *synchronize) createFileItems(dataPath string, headerMap HeaderMap) (map[string]common.SyncFileItem, error) {
	b, err := block.NewManager(dataPath, s.logger)
	if err != nil {
		return nil, err
	}

	fileItems := make(map[string]common.SyncFileItem)

	if err := dnc.Traverse(dataPath, func(info os.FileInfo) error {
		return b.File(info.Name(), func(file block.File) error {
			size, err := file.Size()
			if err != nil {
				return err
			}

			usage, has := headerMap[info.Name()]
			if !has {
				usage = file.Usage()
			}

			fileItems[info.Name()] =
				common.SyncFileItem{
					Sha512Hex: info.Name(),
					Usage:     usage,
					Size:      int32(size),
				}

			return nil
		})
	}); err != nil {
		return nil, err
	}

	return fileItems, nil
}

func (s *synchronize) Start(sourceAddr string) (syncErr error) {
	s.syncMutex.Lock()
	defer s.syncMutex.Unlock()

	s.logger.Sugar().Infof("Sync is in progress...")
	defer func() {
		if syncErr == nil {
			s.logger.Sugar().Infof("Sync is completed.")
		}
	}()

	sourceNode, has := s.nodeCache[sourceAddr]
	if !has {
		var err error
		sourceNode, err = cluster.NewDataNode(sourceAddr)
		if err != nil {
			return err
		}
		s.nodeCache[sourceAddr] = sourceNode
	}

	currentContainer, err := s.createContainer()
	if err != nil {
		return err
	}

	sourceContainer, err := sourceNode.SyncList()
	if err != nil {
		return err
	}

	if err := s.syncFileItems(sourceNode, nil, currentContainer.FileItems, sourceContainer.FileItems); err != nil {
		return err
	}

	if len(currentContainer.Snapshots) > 0 || len(sourceContainer.Snapshots) > 0 {
		s.logger.Sugar().Infof("Snapshot Sync will complete in background...")
		go s.syncSnapshots(sourceNode, currentContainer, sourceContainer)
	}

	return nil
}

func (s *synchronize) syncFileItems(sourceNode cluster.DataNode, snapshotTime *time.Time, currentFileItems map[string]common.SyncFileItem, sourceFileItems map[string]common.SyncFileItem) error {
	createList := make(common.SyncFileItemList, 0)

	for sha512Hex, sourceFileItem := range sourceFileItems {
		currentFileItem, has := currentFileItems[sha512Hex]
		if !has {
			createList = append(createList, sourceFileItem)
			continue
		}

		if !common.CompareFileItems(currentFileItem, sourceFileItem) {
			createList = append(createList, sourceFileItem)
		}

		delete(currentFileItems, sha512Hex)
	}

	wipeList := make(common.SyncFileItemList, 0)

	if len(currentFileItems) > 0 {
		for _, fileItem := range currentFileItems {
			wipeList = append(wipeList, fileItem)
		}
	}

	if len(createList) == 0 && len(wipeList) == 0 {
		return nil
	}

	dataPath := s.rootPath
	syncLoc := "ROOT"
	if snapshotTime != nil {
		snapshotPathName := s.snapshot.PathName(*snapshotTime)
		dataPath = path.Join(s.rootPath, snapshotPathName)

		syncLoc = fmt.Sprintf("SNAPSHOT %s", snapshotTime.Format("2006 Jan 02 15:04:05"))
	}
	s.logger.Sugar().Infof("Sync (%s) will, create: %d / delete: %d", syncLoc, len(createList), len(wipeList))

	b, err := block.NewManager(dataPath, s.logger)
	if err != nil {
		return err
	}

	wg := &sync.WaitGroup{}

	wg.Add(1)
	go s.delete(wg, b, wipeList)

	wg.Add(1)
	go s.create(wg, sourceNode, snapshotTime, b, createList)

	wg.Wait()

	if snapshotTime != nil {
		headerMap := make(HeaderMap)
		for sha512Hex, fileItem := range sourceFileItems {
			headerMap[sha512Hex] = fileItem.Usage
		}
		return s.snapshot.ReplaceHeaderBackup(*snapshotTime, headerMap)
	}

	return nil
}

func (s *synchronize) syncSnapshots(sourceNode cluster.DataNode, currentContainer *common.SyncContainer, sourceContainer *common.SyncContainer) {
	var err error

	s.logger.Sugar().Infof("Snapshot Sync is in progress...")
	defer func() {
		if err != nil {
			s.logger.Error("Snapshot Sync is failed.", zap.Error(err))
			return
		}

		s.logger.Sugar().Infof("Snapshot Sync is completed.")
	}()

	createSnapshotAndSyncFunc := func(sourceSnapshotContainer *common.SnapshotContainer) error {
		if _, err := s.snapshot.Create(&sourceSnapshotContainer.Date); err != nil {
			return err
		}

		snapshotPathName := s.snapshot.PathName(sourceSnapshotContainer.Date)
		snapshotPath := path.Join(s.rootPath, snapshotPathName)

		snapshotFileItems, err := s.createFileItems(snapshotPath, make(HeaderMap))
		if err != nil {
			return err
		}

		if err := s.syncFileItems(sourceNode, &sourceSnapshotContainer.Date, snapshotFileItems, sourceSnapshotContainer.FileItems); err != nil {
			return err
		}

		return nil
	}

	for len(currentContainer.Snapshots) > 0 {
		currentSnapshotContainer := currentContainer.Snapshots[0]

		if len(sourceContainer.Snapshots) == 0 {
			_ = s.snapshot.Delete(currentSnapshotContainer.Date)
			currentContainer.Snapshots = currentContainer.Snapshots[1:]

			continue
		}

		sourceSnapshotContainer := sourceContainer.Snapshots[0]

		if currentSnapshotContainer.Date.Equal(sourceSnapshotContainer.Date) {
			if err = s.syncFileItems(sourceNode, &sourceSnapshotContainer.Date, currentSnapshotContainer.FileItems, sourceSnapshotContainer.FileItems); err != nil {
				return
			}

			currentContainer.Snapshots = currentContainer.Snapshots[1:]
			sourceContainer.Snapshots = sourceContainer.Snapshots[1:]

			continue
		}

		if currentSnapshotContainer.Date.Before(sourceSnapshotContainer.Date) {
			_ = s.snapshot.Delete(currentSnapshotContainer.Date)
			currentContainer.Snapshots = currentContainer.Snapshots[1:]

			continue
		}

		// Create snapshot from root then sync
		if err = createSnapshotAndSyncFunc(sourceSnapshotContainer); err != nil {
			return
		}

		sourceContainer.Snapshots = sourceContainer.Snapshots[1:]
	}

	for len(sourceContainer.Snapshots) > 0 {
		sourceSnapshotContainer := sourceContainer.Snapshots[0]

		if err = createSnapshotAndSyncFunc(sourceSnapshotContainer); err != nil {
			return
		}

		sourceContainer.Snapshots = sourceContainer.Snapshots[1:]
	}
}

func (s *synchronize) delete(wg *sync.WaitGroup, b block.Manager, wipeList common.SyncFileItemList) {
	defer wg.Done()

	totalWipeCount := len(wipeList)
	for len(wipeList) > 0 {
		fileItem := wipeList[0]

		if err := b.File(fileItem.Sha512Hex, func(blockFile block.File) error {
			if blockFile.Temporary() {
				return nil
			}
			return blockFile.Wipe()
		}); err != nil {
			s.logger.Error(
				"Sync cannot delete",
				zap.String("sha512Hex", fileItem.Sha512Hex),
				zap.Int("current", totalWipeCount-(len(wipeList)-1)),
				zap.Int("total", totalWipeCount),
				zap.Error(err),
			)
		}
		wipeList = wipeList[1:]
	}
}

func (s *synchronize) create(wg *sync.WaitGroup, sourceNode cluster.DataNode, snapshotTime *time.Time, b block.Manager, createList common.SyncFileItemList) {
	defer wg.Done()

	totalCreateCount := len(createList)
	for len(createList) > 0 {
		fileItem := createList[0]

		if err := s.createBlockFile(sourceNode, snapshotTime, b, fileItem, totalCreateCount-(len(createList)-1), totalCreateCount); err != nil {
			s.logger.Error(
				"Sync cannot create",
				zap.String("sha512Hex", fileItem.Sha512Hex),
				zap.Int("current", totalCreateCount-(len(createList)-1)),
				zap.Int("total", totalCreateCount),
				zap.Error(err),
			)
		}
		createList = createList[1:]
	}
}

func (s *synchronize) createBlockFile(sourceNode cluster.DataNode, snapshotTime *time.Time, b block.Manager, fileItem common.SyncFileItem, current int, total int) (resultError error) {
	s.logger.Info(
		"Syncing...",
		zap.String("sha512Hex", fileItem.Sha512Hex),
		zap.Int("current", current),
		zap.Int("total", total),
	)
	defer func() {
		if resultError != nil && resultError != errors.ErrQuit {
			s.logger.Error(
				"Sync is failed",
				zap.String("sha512Hex", fileItem.Sha512Hex),
				zap.Int("current", current),
				zap.Int("total", total),
				zap.Error(resultError),
			)
			return
		}

		s.logger.Info(
			"Synced",
			zap.String("sha512Hex", fileItem.Sha512Hex),
			zap.Int("current", current),
			zap.Int("total", total),
		)
	}()

	snapshotTimeUint64 := uint64(0)
	if snapshotTime != nil {
		snapshotTimeUint64 = s.snapshot.ToUint(*snapshotTime)
	}

	return b.File(fileItem.Sha512Hex, func(blockFile block.File) error {
		usageCountBackup := uint16(1)

		return sourceNode.SyncRead(
			snapshotTimeUint64,
			fileItem.Sha512Hex,
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
					return blockFile.Truncate(blockSize) == nil
				}

				return blockFile.ResetUsage(usageCount) != nil
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

				return blockFile.Verify()
			})
	})
}

var _ Synchronize = &synchronize{}
