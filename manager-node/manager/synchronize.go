package manager

import (
	"fmt"
	"sync"

	"github.com/freakmaxi/kertish-dfs/basics/common"
	"github.com/freakmaxi/kertish-dfs/basics/errors"
	cluster2 "github.com/freakmaxi/kertish-dfs/manager-node/cluster"
	"github.com/freakmaxi/kertish-dfs/manager-node/data"
	"go.uber.org/zap"
)

type Synchronize interface {
	QueueClusters(force bool) error
	QueueCluster(clusterId string, force bool)

	Cluster(clusterId string, force bool, keepFrozen bool, waitFullSync bool) error
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
	clusterId  string
	force      bool
	keepFrozen bool
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
				if err := s.startSync(so.clusterId, so.force, so.keepFrozen, false); err != nil {
					s.logger.Error(
						"Cluster sync is failed",
						zap.String("clusterId", so.clusterId),
						zap.Bool("keepFrozen", so.keepFrozen),
						zap.Bool("force", so.force),
						zap.Error(err),
					)
				}
			}(so)
		}
	}()
}

func (s *synchronize) QueueClusters(force bool) error {
	clusters, err := s.clusters.GetAll()
	if err != nil {
		return err
	}

	for _, cluster := range clusters {
		s.QueueCluster(cluster.Id, force)
	}

	return nil
}

func (s *synchronize) QueueCluster(clusterId string, force bool) {
	s.syncChan <- syncOrder{
		clusterId:  clusterId,
		force:      force,
		keepFrozen: false,
	}
}

func (s *synchronize) Cluster(clusterId string, force bool, keepFrozen bool, waitFullSync bool) error {
	return s.startSync(clusterId, force, keepFrozen, waitFullSync)
}

func (s *synchronize) startSync(clusterId string, force bool, keepFrozen bool, waitFullSync bool) error {
	cluster, err := s.clusters.Get(clusterId)
	if err != nil {
		return err
	}

	if !force && cluster.Frozen {
		return errors.ErrFrozen
	}

	if err := s.clusters.SetFreeze(clusterId, true); err != nil {
		return err
	}
	defer func() {
		if keepFrozen {
			return
		}

		if err := s.clusters.SetFreeze(clusterId, false); err != nil {
			s.logger.Error(
				"Syncing error: unfreezing is failed",
				zap.String("clusterId", clusterId),
				zap.Error(err),
			)
		}
	}()

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

	// this changes will save in reset stats
	cluster.Reservations = make(map[string]uint64)
	cluster.Used, _ = mdn.Used()
	cluster.Snapshots = container.Snapshots
	// ---

	s.logger.Info(
		"Resetting cluster stats",
		zap.String("clusterId", clusterId),
		zap.String("nodeId", masterNode.Id),
		zap.String("nodeAddress", masterNode.Address),
	)

	_ = s.clusters.ResetStats(cluster)

	s.logger.Info(
		"Dropping cluster index map",
		zap.String("clusterId", clusterId),
		zap.String("nodeId", masterNode.Id),
		zap.String("nodeAddress", masterNode.Address),
	)

	if err := s.index.DropMap(clusterId); err != nil {
		return errors.ErrSync
	}

	cacheFileItems := make(common.CacheFileItemMap)

	for _, fileItem := range container.FileItems {
		cacheFileItems[fileItem.Sha512Hex] = common.NewCacheFileItem(clusterId, masterNode.Id, fileItem)
	}

	s.logger.Info(
		"Replacing cluster index map",
		zap.String("clusterId", clusterId),
		zap.String("nodeId", masterNode.Id),
		zap.String("nodeAddress", masterNode.Address),
	)

	if err := s.index.ReplaceBulk(cacheFileItems); err != nil {
		s.logger.Error(
			"Index replacement error",
			zap.String("clusterId", clusterId),
			zap.String("nodeId", masterNode.Id),
			zap.String("nodeAddress", masterNode.Address),
			zap.Error(err),
		)
		return errors.ErrPing
	}

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
		if !waitFullSync {
			go s.syncSlaveNode(nil, clusterId, masterNode, slaveNode)
			continue
		}

		wg.Add(1)
		go s.syncSlaveNode(wg, clusterId, masterNode, slaveNode)
	}
	wg.Wait()

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

	sha512HexList := make([]string, 0)
	for sha512Hex := range container.FileItems {
		sha512HexList = append(sha512HexList, sha512Hex)
	}

	if err := s.index.UpdateChunkNodeBulk(sha512HexList, slaveNode.Id, true); err != nil {
		s.logger.Error(
			"Index update error",
			zap.String("clusterId", clusterId),
			zap.String("nodeId", slaveNode.Id),
			zap.String("nodeAddress", slaveNode.Address),
			zap.Error(err),
		)
	}

	s.logger.Info(
		fmt.Sprintf("Synchronization slave node %s of cluster %s is completed", slaveNode.Id, clusterId),
		zap.String("clusterId", clusterId),
		zap.String("nodeId", slaveNode.Id),
		zap.String("nodeAddress", slaveNode.Address),
	)
}

var _ Synchronize = &synchronize{}
