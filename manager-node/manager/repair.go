package manager

import (
	"fmt"
	"os"
	"sync"

	"github.com/freakmaxi/kertish-dfs/basics/common"
	"github.com/freakmaxi/kertish-dfs/basics/errors"
	cluster2 "github.com/freakmaxi/kertish-dfs/manager-node/cluster"
	"github.com/freakmaxi/kertish-dfs/manager-node/data"
	"go.uber.org/zap"
)

const parallelRepair = 10

type RepairType int

const (
	RT_Full      RepairType = 1
	RT_Structure RepairType = 2
	RT_Integrity RepairType = 3
)

type Repair interface {
	Start(repairType RepairType) error
	Status() data.RepairDetail
}

type repair struct {
	clusters    data.Clusters
	metadata    data.Metadata
	index       data.Index
	operation   data.Operation
	synchronize Synchronize
	logger      *zap.Logger
}

func NewRepair(clusters data.Clusters, metadata data.Metadata, index data.Index, operation data.Operation, synchronize Synchronize, logger *zap.Logger) Repair {
	return &repair{
		clusters:    clusters,
		metadata:    metadata,
		index:       index,
		operation:   operation,
		synchronize: synchronize,
		logger:      logger,
	}
}

func (r *repair) Status() data.RepairDetail {
	v, err := r.operation.RepairDetail()
	if err != nil {
		return data.RepairDetail{Processing: true} // On an error, just return as it is still processing
	}
	return v
}

func (r *repair) Start(repairType RepairType) error {
	if r.Status().Processing {
		return errors.ErrProcessing
	}

	if err := r.operation.SetRepairing(true, false); err != nil {
		return err
	}

	go func() {
		zapRepairType := zap.String("repairType", "full")
		switch repairType {
		case RT_Structure:
			zapRepairType = zap.String("repairType", "structure")
		case RT_Integrity:
			zapRepairType = zap.String("repairType", "integrity")
		}
		r.logger.Info("Consistency repair is started...", zapRepairType)

		if err := r.start(repairType); err != nil {
			_ = r.operation.SetRepairing(false, false)
			r.logger.Error("Consistency repair is failed", zap.Error(err))
			return
		}
		_ = r.operation.SetRepairing(false, true)
		r.logger.Info("Consistency repair is completed!")
	}()

	return nil
}

func (r *repair) start(repairType RepairType) error {
	repairStructure := repairType == RT_Full || repairType == RT_Structure
	repairIntegrity := repairType == RT_Full || repairType == RT_Integrity

	if repairStructure {
		r.logger.Info("Repairing metadata structure consistency...")
		if err := r.repairStructure(); err != nil {
			return err
		}
		r.logger.Info("Metadata structure consistency repair is completed!")
	}

	if repairIntegrity {
		r.logger.Info("Repairing metadata integrity...")
		if err := r.repairIntegrity(); err != nil {
			return err
		}
		r.logger.Info("Metadata integrity repair is completed!")
	}

	return nil
}

func (r *repair) repairStructure() error {
	return r.metadata.LockTree(func(folders []*common.Folder) ([]*common.Folder, error) {
		if len(folders) == 0 {
			return nil, nil
		}

		tree := common.NewTree()
		if err := tree.Fill(folders); err != nil {
			return nil, err
		}
		return tree.Normalize(), nil
	})
}

func (r *repair) repairIntegrity() error {
	r.logger.Info("Integrity repairing requires cluster synchronisation.")

	clusters, err := r.clusters.GetAll()
	if err != nil {
		return err
	}

	syncFailure := false

	wg := &sync.WaitGroup{}
	for _, cluster := range clusters {
		wg.Add(1)
		go func(wg *sync.WaitGroup, clusterId string) {
			defer wg.Done()

			if err := r.synchronize.Cluster(clusterId, true, false); err != nil {
				r.logger.Error("Cluster sync is failed for integrity repair",
					zap.String("clusterId", clusterId),
					zap.Error(err),
				)
				syncFailure = true
			}
		}(wg, cluster.Id)
	}
	wg.Wait()

	if syncFailure {
		return errors.ErrSync
	}

	r.logger.Info("Caching cluster chunk map for integrity exam")

	clusterMap := make(map[string]*common.Cluster)
	matchedFileItemListMap := make(map[string]common.SyncFileItemList)

	for _, cluster := range clusters {
		clusterMap[cluster.Id] = cluster
		matchedFileItemListMap[cluster.Id] = make(common.SyncFileItemList, 0)
	}

	mapMutex := sync.Mutex{}
	appendToMatchedFileItemListMapFunc := func(clusterId string, fileItem *common.SyncFileItem) {
		mapMutex.Lock()
		defer mapMutex.Unlock()

		matchedFileItemListMap[clusterId] = append(matchedFileItemListMap[clusterId], *fileItem)
	}

	r.logger.Info("Start traversing metadata entries for integrity check up")

	if err := r.metadata.Cursor(func(folder *common.Folder) (bool, error) {
		if len(folder.Files) == 0 {
			return false, nil
		}

		for _, file := range folder.Files {
			file.Resurrect()

			if len(file.Chunks) == 0 {
				r.logger.Warn(
					"Every file should have at least one chunk entry, this file does not.",
					zap.String("filePath", folder.Full),
					zap.String("fileName", file.Name),
				)

				file.Size = 0
				file.Zombie = true

				continue
			}

			deletionResult := common.NewDeletionResult()

			for _, chunk := range file.Chunks {
				cacheFileItem, err := r.index.Get(chunk.Hash)
				if err != nil {
					if err != os.ErrNotExist {
						return false, err
					}
					deletionResult.Missing = append(deletionResult.Missing, chunk.Hash)
					continue
				}

				if uint32(cacheFileItem.FileItem.Size) != chunk.Size {
					deletionResult.Missing = append(deletionResult.Missing, chunk.Hash)
					continue
				}

				_, has := clusterMap[cacheFileItem.ClusterId]
				if !has {
					deletionResult.Missing = append(deletionResult.Missing, chunk.Hash)
					continue
				}

				deletionResult.Untouched = append(deletionResult.Untouched, chunk.Hash)
				appendToMatchedFileItemListMapFunc(cacheFileItem.ClusterId, &cacheFileItem.FileItem)
			}
			file.IngestDeletion(deletionResult)

			if file.Zombie {
				r.logger.Warn(
					"A zombie file is found",
					zap.String("filePath", folder.Full),
					zap.String("fileName", file.Name),
				)
			}
		}

		return true, nil
	}, parallelRepair); err != nil {
		return err
	}

	r.logger.Info("Start orphan chunk cleanup on clusters")

	// Make Orphan File Chunk Cleanup
	for clusterId, matchedFileItemList := range matchedFileItemListMap {
		masterNode := clusterMap[clusterId].Master()

		wg.Add(1)
		go r.cleanupOrphan(wg, clusterId, masterNode, matchedFileItemList)
	}
	wg.Wait()

	return nil
}

func (r *repair) cleanupOrphan(wg *sync.WaitGroup, clusterId string, masterNode *common.Node, matchedFileItemList common.SyncFileItemList) {
	defer wg.Done()

	clusterSha512HexMap, err := r.index.PullMap(clusterId)
	if err != nil {
		r.logger.Error(
			"Unable to pull cluster file map",
			zap.String("clusterId", clusterId),
			zap.Error(err),
		)
		return
	}

	for _, fileItem := range matchedFileItemList {
		if _, has := clusterSha512HexMap[fileItem.Sha512Hex]; has {
			delete(clusterSha512HexMap, fileItem.Sha512Hex)
		}
	}

	if len(clusterSha512HexMap) == 0 {
		r.logger.Sugar().Infof("%s does not have orphan chunks", clusterId)
		return
	}

	clusterSha512HexList := make([]string, 0)
	for k := range clusterSha512HexMap {
		clusterSha512HexList = append(clusterSha512HexList, k)
	}

	r.logger.Warn(
		fmt.Sprintf("Found %d orphan chunk(s) on %s", len(clusterSha512HexList), clusterId),
		zap.Strings("sha512HexList", clusterSha512HexList),
	)

	mdn, err := cluster2.NewDataNode(masterNode.Address)
	if err != nil {
		r.logger.Error(
			"Unable to make connection to master data node for orphan cleanup",
			zap.String("clusterId", clusterId),
			zap.String("nodeId", masterNode.Id),
			zap.String("nodeAddress", masterNode.Address),
			zap.Error(err),
		)
		return
	}

	r.logger.Sugar().Infof("Creating snapshot for %s...", clusterId)

	if !mdn.SnapshotCreate() {
		r.logger.Error("Unable to create snapshot, cleanup is skipped", zap.String("clusterId", clusterId))
		return
	}

	r.logger.Sugar().Infof("Cleaning up orphan chunks in %s...", clusterId)

	for _, sha512Hex := range clusterSha512HexList {
		if err := mdn.Delete(sha512Hex); err != nil {
			r.logger.Error(
				"Deleting orphan chunk is failed",
				zap.String("clusterId", clusterId),
				zap.String("sha512Hex", sha512Hex),
				zap.Error(err),
			)
			continue
		}
		r.logger.Info(
			"Orphan chunk is deleted",
			zap.String("clusterId", clusterId),
			zap.String("sha512Hex", sha512Hex),
		)
	}

	r.logger.Sugar().Infof("Orphan chunks cleanup for %s is completed!", clusterId)

	// Schedule sync cluster for snapshot sync
	r.synchronize.QueueCluster(clusterId, true)
}

var _ Repair = &repair{}
