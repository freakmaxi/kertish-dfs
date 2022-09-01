package manager

import (
	"fmt"
	"sync"
	"time"

	"github.com/freakmaxi/kertish-dfs/basics/common"
	"github.com/freakmaxi/kertish-dfs/basics/errors"
	cluster2 "github.com/freakmaxi/kertish-dfs/manager-node/cluster"
	"github.com/freakmaxi/kertish-dfs/manager-node/data"
	"go.uber.org/zap"
)

type Synchronize interface {
	QueueClusters() error
	QueueCluster(clusterId string, ignoreMaintainMode bool, keepInMaintainMode bool)

	Cluster(clusterId string, ignoreMaintainMode bool, keepInMaintainMode bool, waitFullSync bool) error
}

type synchronize struct {
	clusters data.Clusters
	index    data.Index
	logger   *zap.Logger

	nodeCacheMutex sync.Mutex
	nodeCache      map[string]cluster2.DataNode

	syncChan chan syncOrder
}

type syncOrder struct {
	clusterId          string
	ignoreMaintainMode bool
	keepInMaintainMode bool
}

func NewSynchronize(clusters data.Clusters, index data.Index, logger *zap.Logger) Synchronize {
	s := &synchronize{
		clusters:       clusters,
		index:          index,
		logger:         logger,
		nodeCacheMutex: sync.Mutex{},
		nodeCache:      make(map[string]cluster2.DataNode),
		syncChan:       make(chan syncOrder),
	}
	s.start()

	return s
}

func (s *synchronize) start() {
	go func() {
		for so := range s.syncChan {
			go func(so syncOrder) {
				if err := s.Cluster(so.clusterId, so.ignoreMaintainMode, so.keepInMaintainMode, false); err != nil {
					s.logger.Error(
						"Cluster sync is failed",
						zap.String("clusterId", so.clusterId),
						zap.Error(err),
					)
				}
			}(so)
		}
	}()
}

func (s *synchronize) QueueClusters() error {
	clusters, err := s.clusters.GetAll()
	if err != nil {
		return err
	}

	for _, cluster := range clusters {
		s.QueueCluster(cluster.Id, false, false)
	}

	return nil
}

func (s *synchronize) QueueCluster(clusterId string, ignoreMaintainMode bool, keepInMaintainMode bool) {
	s.syncChan <- syncOrder{
		clusterId:          clusterId,
		ignoreMaintainMode: ignoreMaintainMode,
		keepInMaintainMode: keepInMaintainMode,
	}
}

func (s *synchronize) Cluster(clusterId string, ignoreMaintainMode bool, keepInMaintainMode bool, waitFullSync bool) error {
	cluster, err := s.clusters.Get(clusterId)
	if err != nil {
		return err
	}

	if cluster.Maintain && !ignoreMaintainMode {
		return errors.ErrMaintain
	}
	if cluster.State == common.StateOffline {
		return errors.ErrOffline
	}
	if cluster.Paralyzed {
		return errors.ErrParalyzed
	}

	if err := s.clusters.UpdateMaintain(cluster.Id, true, common.TopicSynchronisation); err != nil {
		return err
	}

	masterNode := cluster.Master()

	s.logger.Info(
		fmt.Sprintf("Synchronization will be started for cluster %s", clusterId),
		zap.String("clusterId", clusterId),
		zap.String("nodeId", masterNode.Id),
		zap.String("nodeAddress", masterNode.Address),
	)

	mdn, err := cluster2.NewDataNode(masterNode.Address)
	if err != nil {
		s.logger.Error(
			"Syncing error: master node is not accessible",
			zap.String("clusterId", clusterId),
			zap.String("nodeId", masterNode.Id),
			zap.String("nodeAddress", masterNode.Address),
			zap.Error(err),
		)
		return err
	}

	s.logger.Info(
		"Querying sync list",
		zap.String("clusterId", clusterId),
		zap.String("nodeId", masterNode.Id),
		zap.String("nodeAddress", masterNode.Address),
	)

	container, err := mdn.SyncList(nil)
	if err != nil {
		s.logger.Error(
			"Syncing error: master node didn't response for SyncList",
			zap.String("clusterId", clusterId),
			zap.String("nodeId", masterNode.Id),
			zap.String("nodeAddress", masterNode.Address),
			zap.Error(err),
		)
		return errors.ErrPing
	}

	s.logger.Info(
		"Resetting cluster stats",
		zap.String("clusterId", clusterId),
		zap.String("nodeId", masterNode.Id),
		zap.String("nodeAddress", masterNode.Address),
	)

	cluster.Reservations.CleanUp()
	cluster.Used, _ = mdn.Used()
	cluster.Snapshots = container.Snapshots

	_ = s.clusters.ResetStats(cluster)

	s.logger.Info(
		"Queueing cluster index map updates",
		zap.String("clusterId", clusterId),
		zap.String("nodeId", masterNode.Id),
		zap.String("nodeAddress", masterNode.Address),
	)

	syncTime := time.Now().UTC()
	for _, fileItem := range container.FileItems {
		s.index.QueueUpsert(common.NewCacheFileItem(clusterId, masterNode.Id, fileItem), &syncTime)
	}
	s.index.WaitQueueCompletion()

	slaveNodes := cluster.Slaves()

	if len(slaveNodes) == 0 {
		return nil
	}

	if !waitFullSync {
		s.logger.Info(
			"Slaves will be sync concurrently in background",
			zap.String("clusterId", clusterId),
			zap.String("nodeId", masterNode.Id),
			zap.String("nodeAddress", masterNode.Address),
		)
	}

	wg := &sync.WaitGroup{}
	for _, slaveNode := range slaveNodes {
		wg.Add(1)
		go s.syncSlaveNode(wg, clusterId, masterNode, slaveNode)
	}
	if waitFullSync {
		wg.Wait()
	}

	go func(wg *sync.WaitGroup, clusterId string, keepInMaintainMode bool) {
		wg.Wait()
		if err := s.clusters.UpdateMaintain(clusterId, keepInMaintainMode, common.TopicNone); err != nil {
			s.logger.Error(
				"Cluster hasn't been taken off the maintain mode. Needs manual action!",
				zap.String("clusterId", clusterId),
				zap.Error(err),
			)
		}
	}(wg, clusterId, keepInMaintainMode)

	s.logger.Info(
		fmt.Sprintf("Synchronization master node of cluster %s is completed", clusterId),
		zap.String("clusterId", clusterId),
		zap.String("nodeId", masterNode.Id),
		zap.String("nodeAddress", masterNode.Address),
	)

	return nil
}

func (s *synchronize) syncSlaveNode(wg *sync.WaitGroup, clusterId string, masterNode *common.Node, slaveNode *common.Node) {
	if wg != nil {
		defer wg.Done()
	}

	sdn, err := cluster2.NewDataNode(slaveNode.Address)
	if err != nil {
		s.logger.Error(
			"Syncing error: slave node is not accessible",
			zap.String("clusterId", clusterId),
			zap.String("nodeId", slaveNode.Id),
			zap.String("nodeAddress", slaveNode.Address),
			zap.Error(err),
		)
		return
	}

	s.logger.Info(
		"Starting sync between slave and master",
		zap.String("clusterId", clusterId),
		zap.String("nodeId", slaveNode.Id),
		zap.String("nodeAddress", slaveNode.Address),
	)

	if !sdn.SyncFull(masterNode.Address) {
		s.logger.Error(
			"Syncing error: SyncFull is unsuccessful on slave node",
			zap.String("clusterId", clusterId),
			zap.String("nodeId", slaveNode.Id),
			zap.String("nodeAddress", slaveNode.Address),
			zap.String("masterAddress", masterNode.Address),
		)
	}

	s.logger.Info(
		"Querying sync list for slave",
		zap.String("clusterId", clusterId),
		zap.String("nodeId", slaveNode.Id),
		zap.String("nodeAddress", slaveNode.Address),
	)

	container, err := sdn.SyncList(nil)
	if err != nil {
		s.logger.Error(
			"Syncing error: slave node didn't response for SyncList",
			zap.String("clusterId", clusterId),
			zap.String("nodeId", slaveNode.Id),
			zap.String("nodeAddress", slaveNode.Address),
			zap.Error(err),
		)
		return
	}

	s.logger.Info(
		"Updating chunk indices",
		zap.String("clusterId", clusterId),
		zap.String("nodeId", slaveNode.Id),
		zap.String("nodeAddress", slaveNode.Address),
	)

	for sha512Hex := range container.FileItems {
		s.index.QueueUpsertChunkNode(sha512Hex, slaveNode.Id)
	}
	s.index.WaitQueueCompletion()

	s.logger.Info(
		fmt.Sprintf("Synchronization slave node %s of cluster %s is completed", slaveNode.Id, clusterId),
		zap.String("clusterId", clusterId),
		zap.String("nodeId", slaveNode.Id),
		zap.String("nodeAddress", slaveNode.Address),
	)
}

var _ Synchronize = &synchronize{}
