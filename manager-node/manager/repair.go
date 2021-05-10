package manager

import (
	"crypto/sha512"
	"encoding/hex"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
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
	RTFull        RepairType = 1
	RTStructureL1 RepairType = 2
	RTStructureL2 RepairType = 3
	RTIntegrityL1 RepairType = 4
	RTIntegrityL2 RepairType = 5
	RTChecksumL1  RepairType = 6
	RTChecksumL2  RepairType = 7
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
		case RTStructureL1:
			zapRepairType = zap.String("repairType", "structure")
		case RTStructureL2:
			zapRepairType = zap.String("repairType", "structure with integrity")
		case RTIntegrityL1:
			zapRepairType = zap.String("repairType", "integrity")
		case RTIntegrityL2:
			zapRepairType = zap.String("repairType", "integrity with rebuilding checksum calculation")
		case RTChecksumL1:
			zapRepairType = zap.String("repairType", "checksum calculation")
		case RTChecksumL2:
			zapRepairType = zap.String("repairType", "rebuilding checksum calculation")
		}
		r.logger.Info("Consistency repair is started...", zapRepairType)

		if err := r.start(repairType); err != nil {
			_ = r.operation.SetRepairing(false, false)
			r.logger.Error("Consistency repair is failed", zap.Error(err))
			return
		}
		_ = r.operation.SetRepairing(false, true)
		r.logger.Info("Consistency repair is completed")
	}()

	return nil
}

func (r *repair) start(repairType RepairType) error {
	repairStructure := repairType == RTFull || repairType == RTStructureL1 || repairType == RTStructureL2
	repairIntegrity := repairType == RTFull || repairType == RTStructureL2 || repairType == RTIntegrityL1 || repairType == RTIntegrityL2
	repairChecksum := repairType == RTChecksumL1 || repairType == RTChecksumL2

	if repairStructure {
		r.logger.Info("Repairing metadata structure consistency...")
		if err := r.repairStructure(); err != nil {
			return err
		}
		r.logger.Info("Metadata structure consistency repair is completed")
	}

	if repairIntegrity {
		r.logger.Info("Repairing metadata integrity...")
		if err := r.repairIntegrity(repairType == RTFull || repairType == RTIntegrityL2); err != nil {
			return err
		}
		r.logger.Info("Metadata integrity repair is completed")
	}

	if repairChecksum {
		r.logger.Info("Repairing metadata file checksum...")
		if err := r.repairChecksum(repairType == RTChecksumL2); err != nil {
			return err
		}
		r.logger.Info("Metadata file checksum repair is completed")
	}

	return nil
}

func (r *repair) repairStructure() error {
	return r.metadata.LockTree(func(folders []*common.Folder) ([]*common.Folder, error) {
		if len(folders) == 0 {
			return nil, nil
		}

		tree := common.NewTree()
		if err := tree.Fill(nil, folders); err != nil {
			return nil, err
		}
		return tree.Normalize(), nil
	})
}

func (r *repair) repairIntegrity(calculateChecksum bool) error {
	clusters, err := r.clusters.GetAll()
	if err != nil {
		return err
	}

	r.logger.Info("Integrity repairing requires cluster synchronisation.")

	r.metadata.Lock()

	clusterIndexMap, err := r.createClusterIndexMap(clusters, true)
	if err != nil {
		r.metadata.Unlock()
		return err
	}

	clusterMap := make(map[string]*common.Cluster)

	for _, cluster := range clusters {
		clusterMap[cluster.Id] = cluster
	}

	r.logger.Info("Phase 1: Repairing metadata usage alignment...")
	if err := r.repairIntegrityPhase1(clusterIndexMap, clusterMap); err != nil {
		r.metadata.Unlock()
		return err
	}

	r.metadata.Unlock()

	r.logger.Info("Phase 2: Repairing metadata chunk integrity...")
	if err := r.repairIntegrityPhase2(calculateChecksum, clusterIndexMap, clusterMap); err != nil {
		return err
	}

	return nil
}

func (r *repair) createClusterIndexMap(clusters common.Clusters, waitFullSync bool) (map[string]map[string]string, error) {
	syncFailure := false

	clusterIndexMap := make(map[string]map[string]string)

	mapMutex := sync.Mutex{}
	addIndexMapFunc := func(clusterId string, indexMap map[string]string) {
		mapMutex.Lock()
		defer mapMutex.Unlock()

		clusterIndexMap[clusterId] = indexMap
	}

	wg := &sync.WaitGroup{}
	for _, cluster := range clusters {
		wg.Add(1)
		go func(wg *sync.WaitGroup, clusterId string) {
			defer wg.Done()

			if err := r.synchronize.Cluster(clusterId, true, false, waitFullSync); err != nil {
				r.logger.Error("Cluster sync is failed for integrity repair",
					zap.String("clusterId", clusterId),
					zap.Error(err),
				)
				syncFailure = true
				return
			}

			r.logger.Info("Caching cluster chunk map for integrity exam")

			indexMap, err := r.index.PullMap(clusterId)
			if err != nil {
				r.logger.Error("Cluster chunk map caching is failed for integrity repair",
					zap.String("clusterId", clusterId),
					zap.Error(err),
				)
				syncFailure = true
				return
			}
			addIndexMapFunc(clusterId, indexMap)
		}(wg, cluster.Id)
	}
	wg.Wait()

	if syncFailure {
		return nil, errors.ErrSync
	}

	return clusterIndexMap, nil
}

func (r *repair) repairIntegrityPhase1(clusterIndexMap map[string]map[string]string, clusterMap map[string]*common.Cluster) error {
	indexUsageMap := make(map[string]string)

	r.logger.Info("Normalizing cluster indices")

	// Normalize
	for clusterId, chunkIndexMap := range clusterIndexMap {
		for sha512Hex, indexValue := range chunkIndexMap {
			pipeIdx := strings.Index(indexValue, "|")
			if pipeIdx == -1 {
				return fmt.Errorf("faulty index entry for %s", sha512Hex)
			}
			indexUsageMap[sha512Hex] = fmt.Sprintf("%s|%s", indexValue[:pipeIdx], clusterId)
		}
	}

	metadataUsageMapMutex := sync.Mutex{}
	metadataUsageMap := make(map[string]uint16)
	increaseUsageMapFunc := func(sha512Hex string) {
		metadataUsageMapMutex.Lock()
		defer metadataUsageMapMutex.Unlock()

		if _, has := metadataUsageMap[sha512Hex]; !has {
			metadataUsageMap[sha512Hex] = 0
		}
		metadataUsageMap[sha512Hex]++
	}

	r.logger.Info("Start traversing metadata entries for usage alignment cache")

	if err := r.metadata.Cursor(func(folder *common.Folder) (bool, error) {
		if len(folder.Files) == 0 {
			return false, nil
		}

		for _, file := range folder.Files {
			for _, chunk := range file.Chunks {
				increaseUsageMapFunc(chunk.Hash)
			}

			// Cache missing hashes in case of index matching
			for _, chunk := range file.Missing {
				increaseUsageMapFunc(chunk.Hash)
			}
		}

		return false, nil
	}, parallelRepair); err != nil {
		return err
	}

	r.logger.Info("Examine usages of metadata entries with data nodes")

	mismatchedUsageMap := make(map[string]map[string]uint16)

	for sha512Hex, metadataUsage := range metadataUsageMap {
		indexValue, has := indexUsageMap[sha512Hex]
		if !has {
			r.logger.Warn(
				fmt.Sprintf("Found a possible zombie file (%s), phase 2 may fix...", sha512Hex),
				zap.String("sha512Hex", sha512Hex),
			)
			continue
		}

		pipeIdx := strings.Index(indexValue, "|")
		if pipeIdx == -1 {
			return fmt.Errorf("faulty index entry for %s", sha512Hex)
		}

		indexUsage, err := strconv.ParseUint(indexValue[:pipeIdx], 10, 16)
		if err != nil {
			return err
		}

		if metadataUsage == uint16(indexUsage) {
			continue
		}

		indexClusterId := indexValue[pipeIdx+1:]
		cluster, has := clusterMap[indexClusterId]
		if !has {
			r.logger.Error(
				"Metadata chunk is registered but cluster does not exists.",
				zap.String("sha512Hex", sha512Hex),
				zap.String("clusterId", indexClusterId),
			)
			continue
		}

		if _, has := mismatchedUsageMap[cluster.Id]; !has {
			mismatchedUsageMap[cluster.Id] = make(map[string]uint16)
		}
		mismatchedUsageMap[cluster.Id][sha512Hex] = metadataUsage

		r.logger.Warn(
			fmt.Sprintf("Found mismatching usage for %s, expected: %d, found: %d", sha512Hex, metadataUsage, indexUsage),
			zap.String("sha512Hex", sha512Hex),
			zap.Uint16("metadataUsage", metadataUsage),
			zap.Uint16("indexUsage", uint16(indexUsage)),
		)
	}

	if len(mismatchedUsageMap) == 0 {
		r.logger.Info("Metadata and data nodes are perfectly aligned, nothing to do here...")
		return nil
	}

	r.logger.Info("Start usage resetting on clusters")

	// Make Chunk Usage Update
	errCh := make(chan error, len(mismatchedUsageMap))
	wg := &sync.WaitGroup{}
	for clusterId, usageMap := range mismatchedUsageMap {
		masterNode := clusterMap[clusterId].Master()

		wg.Add(1)
		go r.fixUsage(wg, clusterId, masterNode, usageMap, errCh)
	}
	wg.Wait()
	close(errCh)

	if len(errCh) > 0 {
		bulkError := errors.NewBulkError()
		for err := range errCh {
			bulkError.Add(err)
		}
		bulkError.Add(fmt.Errorf("resetting usage is failed"))
		return bulkError
	}

	return nil
}

func (r *repair) fixUsage(wg *sync.WaitGroup, clusterId string, masterNode *common.Node, usageMap map[string]uint16, errCh chan error) {
	defer wg.Done()

	mdn, err := cluster2.NewDataNode(masterNode.Address)
	if err != nil {
		r.logger.Error(
			"Unable to make connection to master data node for usage reset",
			zap.String("clusterId", clusterId),
			zap.String("nodeId", masterNode.Id),
			zap.String("nodeAddress", masterNode.Address),
			zap.Error(err),
		)
		errCh <- err
		return
	}

	if err := mdn.SyncUsage(usageMap); err != nil {
		r.logger.Error(
			"Resetting usage on master data node is failed",
			zap.String("clusterId", clusterId),
			zap.String("nodeId", masterNode.Id),
			zap.String("nodeAddress", masterNode.Address),
			zap.Error(err),
		)
		errCh <- err
		return
	}
}

func (r *repair) repairIntegrityPhase2(calculateChecksum bool, clusterIndexMap map[string]map[string]string, clusterMap map[string]*common.Cluster) error {
	clusterIndexMapMutex := sync.Mutex{}
	deleteFromIndexMapFunc := func(clusterId string, sha512Hex string) {
		clusterIndexMapMutex.Lock()
		defer clusterIndexMapMutex.Unlock()

		delete(clusterIndexMap[clusterId], sha512Hex)
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

			sha512Failed := false
			sha512Hash := sha512.New512_256()

			sort.Sort(file.Chunks)
			for _, chunk := range file.Chunks {
				cacheFileItem, err := r.index.Get(chunk.Hash)
				if err != nil {
					if err != os.ErrNotExist {
						return false, err
					}
					deletionResult.Missing = append(deletionResult.Missing, chunk.Hash)
					continue
				}

				if cacheFileItem.FileItem.Size != chunk.Size {
					deletionResult.Missing = append(deletionResult.Missing, chunk.Hash)
					continue
				}

				_, has := clusterMap[cacheFileItem.ClusterId]
				if !has {
					deletionResult.Missing = append(deletionResult.Missing, chunk.Hash)
					continue
				}

				deletionResult.Untouched = append(deletionResult.Untouched, chunk.Hash)
				deleteFromIndexMapFunc(cacheFileItem.ClusterId, cacheFileItem.FileItem.Sha512Hex)

				if !calculateChecksum {
					continue
				}

				masterNode := clusterMap[cacheFileItem.ClusterId].Master()
				mdn, err := cluster2.NewDataNode(masterNode.Address)
				if err != nil {
					r.logger.Error(
						"Unable to make connection to master data node for checksum calculation",
						zap.String("clusterId", cacheFileItem.ClusterId),
						zap.String("nodeId", masterNode.Id),
						zap.String("nodeAddress", masterNode.Address),
						zap.Error(err),
					)
					sha512Failed = true
					continue
				}

				if err := mdn.Read(chunk.Hash, func(data []byte) error {
					_, err := sha512Hash.Write(data)
					return err
				}); err != nil {
					r.logger.Error(
						fmt.Sprintf("Reading chunk %s from %s is failed, skipping checksum calculation for %s.", chunk.Hash, cacheFileItem.ClusterId, file.Name),
						zap.String("clusterId", cacheFileItem.ClusterId),
						zap.String("sha512Hex", chunk.Hash),
						zap.Error(err),
					)
					sha512Failed = true
				}
			}
			file.IngestDeletion(deletionResult)

			if file.Zombie {
				r.logger.Warn(
					"A zombie file is found",
					zap.String("filePath", folder.Full),
					zap.String("fileName", file.Name),
				)
				continue
			}

			if calculateChecksum {
				if sha512Failed {
					r.logger.Warn(
						fmt.Sprintf("Updating checksum of %s is not possible because of the failure(s) on calculation operation", file.Name),
						zap.String("filePath", folder.Full),
						zap.String("filename", file.Name),
					)
					continue
				}
				file.Checksum = hex.EncodeToString(sha512Hash.Sum(nil))
			}
		}

		return true, nil
	}, parallelRepair); err != nil {
		return err
	}

	r.logger.Info("Start orphan chunk cleanup on clusters")

	// Make Orphan File Chunk Cleanup
	wg := &sync.WaitGroup{}
	for clusterId, indexMap := range clusterIndexMap {
		masterNode := clusterMap[clusterId].Master()

		wg.Add(1)
		go r.cleanupOrphan(wg, clusterId, masterNode, indexMap)
	}
	wg.Wait()

	return nil
}

func (r *repair) cleanupOrphan(wg *sync.WaitGroup, clusterId string, masterNode *common.Node, indexMap map[string]string) {
	defer wg.Done()

	if len(indexMap) == 0 {
		r.logger.Info(fmt.Sprintf("%s does not have orphan chunks", clusterId))
		return
	}

	clusterSha512HexList := make([]string, 0)
	for k := range indexMap {
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

	r.logger.Info(fmt.Sprintf("Creating snapshot for %s...", clusterId))

	if !mdn.SnapshotCreate() {
		r.logger.Error("Unable to create snapshot, cleanup is skipped", zap.String("clusterId", clusterId))
		return
	}

	r.logger.Info(fmt.Sprintf("Cleaning up orphan chunks in %s...", clusterId))

	for _, sha512Hex := range clusterSha512HexList {
		if err := mdn.Delete(sha512Hex); err != nil {
			r.logger.Error(
				fmt.Sprintf("Deleting orphan chunk %s from %s is failed", sha512Hex, clusterId),
				zap.String("clusterId", clusterId),
				zap.String("sha512Hex", sha512Hex),
				zap.Error(err),
			)
			continue
		}
		r.logger.Info(
			fmt.Sprintf("Orphan chunk %s from %s is deleted", sha512Hex, clusterId),
			zap.String("clusterId", clusterId),
			zap.String("sha512Hex", sha512Hex),
		)
	}

	r.logger.Info(fmt.Sprintf("Orphan chunks cleanup for %s is completed", clusterId))

	// Schedule sync cluster for snapshot sync
	r.synchronize.QueueCluster(clusterId, true)
}

func (r *repair) repairChecksum(rebuildChecksum bool) error {
	clusters, err := r.clusters.GetAll()
	if err != nil {
		return err
	}

	r.logger.Info("Checksum repairing requires cluster synchronisation.")

	r.metadata.Lock()

	if _, err := r.createClusterIndexMap(clusters, false); err != nil {
		return err
	}

	clusterMap := make(map[string]*common.Cluster)

	for _, cluster := range clusters {
		clusterMap[cluster.Id] = cluster
	}

	r.metadata.Unlock()

	r.logger.Info("Repairing metadata file checksum...")
	if err := r.repairChecksumCalculation(rebuildChecksum, clusterMap); err != nil {
		return err
	}

	return nil
}

func (r *repair) repairChecksumCalculation(rebuildChecksum bool, clusterMap map[string]*common.Cluster) error {
	r.logger.Info("Start traversing metadata entries for checksum check up")

	if err := r.metadata.Cursor(func(folder *common.Folder) (bool, error) {
		if len(folder.Files) == 0 {
			return false, nil
		}

		updatedChecksum := 0
		for _, file := range folder.Files {
			if !rebuildChecksum && len(file.Checksum) > 0 {
				continue
			}

			if len(file.Chunks) == 0 {
				r.logger.Warn(
					"Every file should have at least one chunk entry, this file does not.",
					zap.String("filePath", folder.Full),
					zap.String("fileName", file.Name),
				)
				continue
			}

			sha512Failed := false
			sha512Hash := sha512.New512_256()

			sort.Sort(file.Chunks)
			for _, chunk := range file.Chunks {
				cacheFileItem, err := r.index.Get(chunk.Hash)
				if err != nil {
					if err != os.ErrNotExist {
						return false, err
					}
					r.logger.Error(
						fmt.Sprintf("File chunk is missing, skipping checksum calculation for %s.", file.Name),
						zap.String("filePath", folder.Full),
						zap.String("fileName", file.Name),
					)
					sha512Failed = true
					break
				}

				if cacheFileItem.FileItem.Size != chunk.Size {
					r.logger.Error(
						fmt.Sprintf("File size mismatched, skipping checksum calculation for %s.", file.Name),
						zap.String("filePath", folder.Full),
						zap.String("fileName", file.Name),
					)
					sha512Failed = true
					break
				}

				_, has := clusterMap[cacheFileItem.ClusterId]
				if !has {
					r.logger.Error(
						fmt.Sprintf("Chunk cluster is not exists, skipping checksum calculation for %s.", file.Name),
						zap.String("clusterId", cacheFileItem.ClusterId),
						zap.String("filePath", folder.Full),
						zap.String("fileName", file.Name),
					)
					sha512Failed = true
					break
				}

				masterNode := clusterMap[cacheFileItem.ClusterId].Master()
				mdn, err := cluster2.NewDataNode(masterNode.Address)
				if err != nil {
					r.logger.Error(
						"Unable to make connection to master data node for checksum calculation",
						zap.String("clusterId", cacheFileItem.ClusterId),
						zap.String("nodeId", masterNode.Id),
						zap.String("nodeAddress", masterNode.Address),
						zap.Error(err),
					)
					sha512Failed = true
					break
				}

				if err := mdn.Read(chunk.Hash, func(data []byte) error {
					_, err := sha512Hash.Write(data)
					return err
				}); err != nil {
					r.logger.Error(
						fmt.Sprintf("Reading chunk %s from %s is failed, skipping checksum calculation for %s.", chunk.Hash, cacheFileItem.ClusterId, file.Name),
						zap.String("clusterId", cacheFileItem.ClusterId),
						zap.String("sha512Hex", chunk.Hash),
						zap.Error(err),
					)
					sha512Failed = true
					break
				}
			}

			if sha512Failed {
				r.logger.Warn(
					fmt.Sprintf("Updating checksum of %s is not possible because of the failure(s) on calculation operation", file.Name),
					zap.String("filePath", folder.Full),
					zap.String("filename", file.Name),
				)
				continue
			}

			file.Checksum = hex.EncodeToString(sha512Hash.Sum(nil))
			updatedChecksum++
		}

		return updatedChecksum > 0, nil
	}, parallelRepair); err != nil {
		return err
	}

	return nil
}

var _ Repair = &repair{}
