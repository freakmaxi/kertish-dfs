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
	List(snapshotTime *time.Time, itemHandler func(fileItem *common.SyncFileItem) error) error

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

func (s *synchronize) List(snapshotTime *time.Time, itemHandler func(fileItem *common.SyncFileItem) error) error {
	s.syncMutex.Lock()
	defer s.syncMutex.Unlock()

	dataPath := s.rootPath
	headerMap := make(HeaderMap)

	if snapshotTime != nil {
		snapshotPathName := s.snapshot.PathName(*snapshotTime)
		dataPath = path.Join(dataPath, snapshotPathName)

		headerMap, _ = s.snapshot.ReadHeaderBackup(*snapshotTime)
	}

	return s.iterateFileItems(dataPath, headerMap, itemHandler)
}

func (s *synchronize) iterateFileItems(dataPath string, headerMap HeaderMap, itemHandler func(fileItem *common.SyncFileItem) error) error {
	b, err := block.NewManager(dataPath, s.logger)
	if err != nil {
		return err
	}

	return dnc.Traverse(dataPath, func(info os.FileInfo) error {
		return b.File(info.Name(), func(file block.File) error {
			size, err := file.Size()
			if err != nil {
				return err
			}

			usage, has := headerMap[info.Name()]
			if !has {
				usage = file.Usage()
			}

			return itemHandler(
				&common.SyncFileItem{
					Sha512Hex: info.Name(),
					Usage:     usage,
					Size:      size,
				},
			)
		})
	})
}

func (s *synchronize) Start(sourceAddr string) error {
	s.syncMutex.Lock()
	defer s.syncMutex.Unlock()

	s.logger.Sugar().Infof("Sync is in progress...")

	sourceNode, has := s.nodeCache[sourceAddr]
	if !has {
		var err error
		sourceNode, err = cluster.NewDataNode(sourceAddr)
		if err != nil {
			return err
		}
		s.nodeCache[sourceAddr] = sourceNode
	}

	sourceContainer, err := sourceNode.SyncList(nil)
	if err != nil {
		return err
	}

	if err := s.syncFileItems(sourceNode, nil, sourceContainer.FileItems); err != nil {
		return err
	}

	go s.syncSnapshots(sourceNode, sourceContainer)

	s.logger.Sugar().Infof("Sync is completed.")

	return nil
}

func (s *synchronize) syncFileItems(sourceNode cluster.DataNode, snapshotTime *time.Time, sourceFileItems common.SyncFileItemMap) error {
	syncLoc := "ROOT"

	dataPath := s.rootPath
	headerMap := make(HeaderMap)
	if snapshotTime != nil {
		snapshotPathName := s.snapshot.PathName(*snapshotTime)
		dataPath = path.Join(dataPath, snapshotPathName)

		headerMap, _ = s.snapshot.ReadHeaderBackup(*snapshotTime)
		syncLoc = fmt.Sprintf("SNAPSHOT %s", snapshotTime.Format(common.FriendlyTimeFormatWithSeconds))
	}

	wipeList := make(common.SyncFileItemList, 0)
	createList := make(common.SyncFileItemList, 0)
	sourceHeaderMap := make(HeaderMap)

	if err := s.iterateFileItems(dataPath, headerMap, func(fileItem *common.SyncFileItem) error {
		sourceFileItem, has := sourceFileItems[fileItem.Sha512Hex]
		if !has {
			wipeList = append(wipeList, *fileItem)
			return nil
		}

		sourceHeaderMap[sourceFileItem.Sha512Hex] = sourceFileItem.Usage

		if !common.CompareFileItems(*fileItem, sourceFileItem) {
			createList = append(createList, sourceFileItem)
		}

		delete(sourceFileItems, sourceFileItem.Sha512Hex)

		return nil
	}); err != nil {
		return err
	}

	if len(sourceFileItems) > 0 {
		for _, fileItem := range sourceFileItems {
			createList = append(createList, fileItem)
		}
	}

	if len(createList) == 0 && len(wipeList) == 0 {
		return nil
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
		return s.snapshot.ReplaceHeaderBackup(*snapshotTime, sourceHeaderMap)
	}

	return nil
}

func (s *synchronize) syncSnapshots(sourceNode cluster.DataNode, sourceContainer *common.SyncContainer) {
	s.logger.Sugar().Infof("Snapshot Sync will complete in background...")

	completed := false

	snapshots, err := s.snapshot.Dates()
	if err != nil {
		s.logger.Error("unable to get snapshot dates for snapshot sync operation", zap.Error(err))
		return
	}

	s.logger.Sugar().Infof("Snapshot Sync is in progress...")
	defer func() {
		if !completed {
			s.logger.Error("Snapshot Sync is failed.")
			return
		}

		s.logger.Sugar().Infof("Snapshot Sync is completed.")
	}()

	if len(snapshots) == 0 && len(sourceContainer.Snapshots) == 0 {
		completed = true
		return
	}

	createSnapshotAndSyncFunc := func(targetSnapshot time.Time) error {
		if _, err := s.snapshot.Create(&targetSnapshot); err != nil {
			s.logger.Error("snapshot creation for sync is failed", zap.Error(err))
			return err
		}

		targetContainer, err := sourceNode.SyncList(&targetSnapshot)
		if err != nil {
			s.logger.Error("request for snapshot sync list is failed", zap.Error(err))
			return err
		}

		if err = s.syncFileItems(sourceNode, &targetSnapshot, targetContainer.FileItems); err != nil {
			s.logger.Error("syncing snapshot files is failed", zap.Error(err))
			return err
		}

		return nil
	}

	for len(snapshots) > 0 {
		currentSnapshot := snapshots[0]

		if len(sourceContainer.Snapshots) == 0 {
			_ = s.snapshot.Delete(currentSnapshot)
			snapshots = snapshots[1:]

			continue
		}

		sourceSnapshot := sourceContainer.Snapshots[0]

		if currentSnapshot.Equal(sourceSnapshot) {
			targetContainer, err := sourceNode.SyncList(&sourceSnapshot)
			if err != nil {
				s.logger.Error("request for snapshot sync list is failed", zap.Error(err))
				return
			}

			if err = s.syncFileItems(sourceNode, &sourceSnapshot, targetContainer.FileItems); err != nil {
				return
			}

			snapshots = snapshots[1:]
			sourceContainer.Snapshots = sourceContainer.Snapshots[1:]

			continue
		}

		if currentSnapshot.Before(sourceSnapshot) {
			_ = s.snapshot.Delete(currentSnapshot)
			snapshots = snapshots[1:]

			continue
		}

		// Create snapshot from root then sync
		if err := createSnapshotAndSyncFunc(sourceSnapshot); err != nil {
			return
		}

		sourceContainer.Snapshots = sourceContainer.Snapshots[1:]
	}

	for len(sourceContainer.Snapshots) > 0 {
		sourceSnapshot := sourceContainer.Snapshots[0]

		if err := createSnapshotAndSyncFunc(sourceSnapshot); err != nil {
			return
		}

		sourceContainer.Snapshots = sourceContainer.Snapshots[1:]
	}

	completed = true
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
		currentCreatedCount := totalCreateCount - (len(createList) - 1)
		if err := s.createBlockFile(sourceNode, snapshotTime, b, fileItem, currentCreatedCount, totalCreateCount); err != nil && err != errors.ErrQuit {
			s.logger.Error(
				fmt.Sprintf("Sync cannot create %s - %d/%d", fileItem.Sha512Hex, currentCreatedCount, totalCreateCount),
				zap.String("sha512Hex", fileItem.Sha512Hex),
				zap.Int("current", currentCreatedCount),
				zap.Int("total", totalCreateCount),
				zap.Error(err),
			)
		}
		createList = createList[1:]
	}
}

func (s *synchronize) createBlockFile(sourceNode cluster.DataNode, snapshotTime *time.Time, b block.Manager, fileItem common.SyncFileItem, current int, total int) (resultError error) {
	if err := b.File(fileItem.Sha512Hex, func(blockFile block.File) error {
		if !blockFile.Temporary() {
			if blockFile.VerifyForce() {
				return blockFile.ResetUsage(fileItem.Usage)
			}

			if err := blockFile.Truncate(fileItem.Size); err != nil {
				return err
			}
		}

		return sourceNode.SyncRead(
			snapshotTime,
			fileItem.Sha512Hex,
			false,
			func(data []byte) error {
				return blockFile.Write(data)
			},
			func(usage uint16) bool {
				if err := blockFile.ResetUsage(usage); err != nil {
					return false
				}

				return blockFile.Verify()
			})
	}); err != nil {
		return err
	}

	s.logger.Info(
		fmt.Sprintf("Synced %s - %d/%d...", fileItem.Sha512Hex, current, total),
		zap.String("sha512Hex", fileItem.Sha512Hex),
		zap.Int("current", current),
		zap.Int("total", total),
	)

	return nil
}

var _ Synchronize = &synchronize{}
