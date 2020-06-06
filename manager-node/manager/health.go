package manager

import (
	"strings"
	"sync"
	"time"

	"github.com/freakmaxi/kertish-dfs/basics/common"
	"github.com/freakmaxi/kertish-dfs/basics/errors"
	cluster2 "github.com/freakmaxi/kertish-dfs/manager-node/cluster"
	"github.com/freakmaxi/kertish-dfs/manager-node/data"
	"go.uber.org/zap"
)

const defaultIntervalDuration = time.Second * 10
const maintainDuration = time.Hour * 24
const parallelRepair = 10

type RepairType int

var (
	RT_Full      RepairType = 1
	RT_Structure RepairType = 2
	RT_Integrity RepairType = 3
)

type Health interface {
	Start()

	SyncClusters() []error
	SyncClusterById(clusterId string) error
	SyncCluster(cluster *common.Cluster, keepFrozen bool) error
	RepairConsistency(repairType RepairType) error

	Operations() Operations
}

type Operations interface {
	RepairDetail() data.RepairDetail
}

type health struct {
	clusters         data.Clusters
	index            data.Index
	metadata         data.Metadata
	operation        data.Operation
	logger           *zap.Logger
	intervalDuration time.Duration

	nodeCacheMutex sync.Mutex
	nodeCache      map[string]cluster2.DataNode
}

func NewHealthTracker(clusters data.Clusters, index data.Index, metadata data.Metadata, operation data.Operation, logger *zap.Logger, intervalDuration time.Duration) Health {
	if intervalDuration == 0 {
		intervalDuration = defaultIntervalDuration
	}

	return &health{
		clusters:         clusters,
		index:            index,
		metadata:         metadata,
		operation:        operation,
		logger:           logger,
		intervalDuration: intervalDuration,
		nodeCacheMutex:   sync.Mutex{},
		nodeCache:        make(map[string]cluster2.DataNode),
	}
}

func (h *health) getDataNode(node *common.Node) (cluster2.DataNode, error) {
	h.nodeCacheMutex.Lock()
	defer h.nodeCacheMutex.Unlock()

	dn, has := h.nodeCache[node.Id]
	if !has {
		var err error
		dn, err = cluster2.NewDataNode(node.Address)
		if err != nil {
			return nil, err
		}
		h.nodeCache[node.Address] = dn
	}

	return dn, nil
}

func (h *health) Start() {
	go h.maintain()
	go h.health()
}

func (h *health) maintain() {
	for {
		select {
		case <-time.After(maintainDuration):
			h.logger.Info("Maintaining Clusters...")
			// Fire Forget
			go func() {
				clusters, err := h.clusters.GetAll()
				if err != nil {
					return
				}

				for _, cluster := range clusters {
					if cluster.Frozen {
						h.logger.Warn("Frozen cluster is skipped to maintain", zap.String("clusterId", cluster.Id))
						continue
					}

					// Do not block all clusters and finished it one by one.
					if err := h.SyncCluster(cluster, false); err != nil {
						h.logger.Error(
							"Syncing cluster in maintain is failed",
							zap.String("clusterId", cluster.Id),
							zap.Error(err),
						)
					}
				}
				h.logger.Info("Maintain is completed!")
			}()
		}
	}
}

func (h *health) health() {
	for {
		select {
		case <-time.After(h.intervalDuration):
			clusters, err := h.clusters.GetAll()
			if err != nil {
				continue
			}

			wg := &sync.WaitGroup{}
			for _, cluster := range clusters {
				if cluster.Frozen {
					continue
				}

				wg.Add(1)
				go h.checkHealth(wg, cluster)
			}
			wg.Wait()
		}
	}
}

func (h *health) checkHealth(wg *sync.WaitGroup, cluster *common.Cluster) {
	defer wg.Done()

	cluster.Paralyzed = false

	if !h.checkMasterAlive(cluster) {
		newMaster := h.findBestMasterNodeCandidate(cluster)
		if newMaster == nil {
			cluster.Paralyzed = true
			_ = h.clusters.UpdateNodes(cluster)

			return
		}

		if strings.Compare(newMaster.Id, cluster.Master().Id) != 0 {
			if err := h.clusters.SetNewMaster(cluster.Id, newMaster.Id); err == nil {
				_ = cluster.SetMaster(newMaster.Id)
				h.notifyNewMasterInCluster(cluster)
			}
		}
	}
	h.prioritizeNodesByConnectionQuality(cluster)
	_ = h.clusters.UpdateNodes(cluster)
}

func (h *health) checkMasterAlive(cluster *common.Cluster) bool {
	masterNode := cluster.Master()

	dn, err := h.getDataNode(masterNode)
	if err != nil {
		h.logger.Warn(
			"Master node live check is failed",
			zap.String("clusterId", cluster.Id),
			zap.String("nodeId", masterNode.Id),
			zap.Error(err),
		)
		return false
	}

	return dn.Ping() > -1
}

func (h *health) findBestMasterNodeCandidate(cluster *common.Cluster) *common.Node {
	for _, node := range cluster.Nodes {
		dn, err := h.getDataNode(node)
		if err != nil {
			h.logger.Warn(
				"Finding best master node candidate is failed",
				zap.String("clusterId", cluster.Id),
				zap.String("nodeId", node.Id),
				zap.Error(err),
			)
			continue
		}

		pr := dn.Ping()

		if pr == -1 {
			continue
		}

		container, err := dn.SyncList()
		if err != nil {
			continue
		}

		failed, err := h.index.Compare(cluster.Id, container.FileItems.ToList())
		if err != nil {
			continue
		}

		if failed == 0 {
			return node
		}
	}
	return nil
}

func (h *health) prioritizeNodesByConnectionQuality(cluster *common.Cluster) {
	for _, node := range cluster.Nodes {
		dn, err := h.getDataNode(node)
		if err != nil {
			h.logger.Warn(
				"Prioritizing node connection quality is failed",
				zap.String("clusterId", cluster.Id),
				zap.String("nodeId", node.Id),
				zap.Error(err),
			)

			node.Quality = int64(^uint(0) >> 1)
			continue
		}

		pr := dn.Ping()

		if pr == -1 {
			node.Quality = int64(^uint(0) >> 1)
			continue
		}
		node.Quality = pr
	}
}

func (h *health) notifyNewMasterInCluster(cluster *common.Cluster) {
	for _, node := range cluster.Nodes {
		dn, err := h.getDataNode(node)
		if err != nil {
			h.logger.Warn(
				"Notifying new master node is failed",
				zap.String("clusterId", cluster.Id),
				zap.String("nodeId", node.Id),
				zap.Error(err),
			)
			continue
		}

		if dn.Ping() == -1 {
			continue
		}
		dn.Mode(node.Master)
	}
}

func (h *health) SyncClusters() []error {
	clusters, err := h.clusters.GetAll()
	if err != nil {
		return []error{err}
	}

	wg := &sync.WaitGroup{}

	errorListMutex := sync.Mutex{}
	errorList := make([]error, 0)
	addErrorFunc := func(err error) {
		errorListMutex.Lock()
		defer errorListMutex.Unlock()

		errorList = append(errorList, err)
	}

	for _, cluster := range clusters {
		wg.Add(1)
		go func(wg *sync.WaitGroup, sc common.Cluster) {
			defer wg.Done()
			for {
				if err := h.SyncCluster(&sc, false); err != nil {
					if err == errors.ErrPing {
						<-time.After(time.Second)
						continue
					}
					addErrorFunc(err)
					return
				}
				return
			}
		}(wg, *cluster)
	}
	wg.Wait()

	return errorList
}

func (h *health) SyncClusterById(clusterId string) error {
	cluster, err := h.clusters.Get(clusterId)
	if err != nil {
		return err
	}
	return h.SyncCluster(cluster, false)
}

func (h *health) SyncCluster(cluster *common.Cluster, keepFrozen bool) error {
	if err := h.clusters.SetFreeze(cluster.Id, true); err != nil {
		return err
	}
	defer func() {
		_ = h.clusters.ResetStats(cluster)
		if keepFrozen {
			return
		}

		if err := h.clusters.SetFreeze(cluster.Id, false); err != nil {
			h.logger.Error(
				"Syncing error: unfreezing is failed",
				zap.String("clusterId", cluster.Id),
				zap.Error(err),
			)
		}
	}()

	masterNode := cluster.Master()
	slaveNodes := cluster.Slaves()

	mdn, err := cluster2.NewDataNode(masterNode.Address)
	if err != nil || !mdn.Join(cluster.Id, masterNode.Id, "") {
		return errors.ErrJoin
	}

	container, err := mdn.SyncList()
	if err != nil {
		h.logger.Error(
			"Syncing error: node didn't response for SyncList",
			zap.String("nodeId", masterNode.Id),
			zap.Error(err),
		)
		return errors.ErrPing
	}

	// this changes will save in reset stats
	cluster.Reservations = make(map[string]uint64)
	cluster.Used, _ = mdn.Used()
	cluster.Snapshots = make(common.Snapshots, 0)
	for _, snapshotContainer := range container.Snapshots {
		cluster.Snapshots = append(cluster.Snapshots, snapshotContainer.Date)
	}
	// ---

	if err := h.index.Replace(cluster.Id, container.FileItems.ToList()); err != nil {
		h.logger.Error(
			"Index replacement error",
			zap.String("clusterId", cluster.Id),
			zap.Error(err),
		)
		return errors.ErrPing
	}

	if len(slaveNodes) == 0 {
		return nil
	}

	wg := &sync.WaitGroup{}
	for _, slaveNode := range slaveNodes {
		wg.Add(1)
		go func(wg *sync.WaitGroup, mN *common.Node, sN *common.Node) {
			defer wg.Done()

			sdn, err := cluster2.NewDataNode(sN.Address)
			if err != nil || !sdn.Join(cluster.Id, sN.Id, masterNode.Address) {
				h.logger.Error(
					"Syncing error",
					zap.Error(errors.ErrJoin),
				)
				return
			}

			if !sdn.SyncFull(mN.Address) {
				h.logger.Error(
					"Syncing node is failed",
					zap.String("slaveNodeId", sN.Id),
					zap.String("masterNodeAddress", mN.Address),
				)
			}
		}(wg, masterNode, slaveNode)
	}
	wg.Wait()

	return nil
}

func (h *health) RepairDetail() data.RepairDetail {
	v, err := h.operation.RepairDetail()
	if err != nil {
		return data.RepairDetail{Processing: true} // On an error, just return as it is still processing
	}
	return v
}

func (h *health) RepairConsistency(repairType RepairType) error {
	if h.RepairDetail().Processing {
		return errors.ErrProcessing
	}

	if err := h.operation.SetRepairing(true, false); err != nil {
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
		h.logger.Info("Consistency repair is started...", zapRepairType)

		if err := h.repairConsistency(repairType); err != nil {
			_ = h.operation.SetRepairing(false, false)
			h.logger.Error("Consistency repair is failed", zap.Error(err))
			return
		}
		_ = h.operation.SetRepairing(false, true)
		h.logger.Info("Consistency repair is completed!")
	}()

	return nil
}

func (h *health) repairConsistency(repairType RepairType) error {
	repairStructure := repairType == RT_Full || repairType == RT_Structure
	repairIntegrity := repairType == RT_Full || repairType == RT_Integrity

	if repairStructure {
		h.logger.Info("Repairing metadata structure consistency...")
		// Repair Structure
		if err := h.repairStructure(); err != nil {
			return err
		}
		h.logger.Info("Metadata structure consistency repair is completed!")
	}

	if !repairIntegrity {
		return nil
	}

	syncErrors := h.SyncClusters()
	if len(syncErrors) > 0 {
		h.logger.Error("Sync is failed for integrity repair", zap.Errors("syncErrors", syncErrors))
		return errors.ErrSync
	}

	h.logger.Info("Repairing metadata integrity...")
	// Repair Integrity
	clusters, err := h.clusters.GetAll()
	if err != nil {
		return err
	}

	matchedFileItemListMap := make(map[string]common.SyncFileItemList)
	clusterIds := make([]string, len(clusters))
	clusterMap := make(map[string]*common.Cluster)
	for i, cluster := range clusters {
		clusterIds[i] = cluster.Id
		clusterMap[cluster.Id] = cluster
		matchedFileItemListMap[cluster.Id] = make(common.SyncFileItemList, 0)
	}

	if err := h.metadata.Cursor(func(folder *common.Folder) (bool, error) {
		changed := false
		for _, file := range folder.Files {
			file.Resurrect()

			if len(file.Chunks) == 0 {
				file.Size = 0
				file.Zombie = true

				changed = true
				break
			}

			deletionResult := common.NewDeletionResult()

			for _, chunk := range file.Chunks {
				clusterId, fileItem, err := h.index.Find(clusterIds, chunk.Hash)
				if err != nil {
					if err != errors.ErrNotFound {
						return false, err
					}
					deletionResult.Missing = append(deletionResult.Missing, chunk.Hash)
					continue
				}

				if uint32(fileItem.Size) != chunk.Size {
					deletionResult.Missing = append(deletionResult.Missing, chunk.Hash)
					continue
				}

				deletionResult.Untouched = append(deletionResult.Untouched, chunk.Hash)
				matchedFileItemListMap[clusterId] = append(matchedFileItemListMap[clusterId], *fileItem)
			}
			file.IngestDeletion(deletionResult)
			changed = true
		}
		return changed, nil
	}, parallelRepair); err != nil {
		return err
	}
	h.logger.Info("Metadata integrity repair is completed!")

	// Make Orphan File Chunk Cleanup
	cleanupOrphanFunc := func(wg *sync.WaitGroup, clusterId string, matchedFileItemList common.SyncFileItemList) {
		defer wg.Done()

		orphanFileItemList, err := h.index.Extract(clusterId, matchedFileItemList)
		if err != nil {
			h.logger.Error(
				"Unable to extract orphan list",
				zap.String("clusterId", clusterId),
				zap.Error(err),
			)
			return
		}

		if len(orphanFileItemList) == 0 {
			return
		}

		masterNode := clusterMap[clusterId].Master()
		mdn, err := cluster2.NewDataNode(masterNode.Address)
		if err != nil {
			h.logger.Error(
				"Unable to make connection to master data node for orphan cleanup",
				zap.String("clusterId", clusterId),
				zap.Error(err),
			)
			return
		}

		h.logger.Sugar().Infof("Cleaning up orphan file chunks for %s...", clusterId)

		if !mdn.SnapshotCreate() {
			h.logger.Error("Unable to create snapshot, cleanup is skipped", zap.String("clusterId", clusterId))
			return
		}

		if err := h.SyncClusterById(clusterId); err != nil {
			h.logger.Error(
				"Unable to sync cluster for snapshot but will be recovered later",
				zap.String("clusterId", clusterId),
				zap.Error(err),
			)
		}

		for _, orphanFileItem := range orphanFileItemList {
			if err := mdn.Delete(orphanFileItem.Sha512Hex); err != nil {
				h.logger.Error(
					"Deleting orphan file is failed",
					zap.String("clusterId", clusterId),
					zap.String("sha512Hex", orphanFileItem.Sha512Hex),
					zap.Uint16("usage", orphanFileItem.Usage),
					zap.Int32("size", orphanFileItem.Size),
					zap.Error(err),
				)
				continue
			}
			h.logger.Info(
				"Orphan file is deleted",
				zap.String("clusterId", clusterId),
				zap.String("sha512Hex", orphanFileItem.Sha512Hex),
				zap.Uint16("usage", orphanFileItem.Usage),
				zap.Int32("size", orphanFileItem.Size),
			)
		}

		h.logger.Sugar().Infof("Orphan file chunks clean up for %s is completed!", clusterId)
	}

	wg := &sync.WaitGroup{}
	for clusterId, matchedFileItemList := range matchedFileItemListMap {
		wg.Add(1)
		go cleanupOrphanFunc(wg, clusterId, matchedFileItemList)
	}
	wg.Wait()

	return nil
}

func (h *health) repairStructure() error {
	return h.metadata.LockTree(func(folders []*common.Folder) ([]*common.Folder, error) {
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

func (h *health) Operations() Operations {
	return h
}
