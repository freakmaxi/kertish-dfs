package manager

import (
	"fmt"
	"sync"

	"github.com/freakmaxi/kertish-dfs/basics/errors"
	cluster2 "github.com/freakmaxi/kertish-dfs/manager-node/cluster"
	"github.com/freakmaxi/kertish-dfs/manager-node/data"
	"go.uber.org/zap"
)

const moveFailureThreshold = 0.001
const moveSemaphoreLimit = 10

type move struct {
	clusters    data.Clusters
	synchronize Synchronize
	logger      *zap.Logger

	semaphoreWG   sync.WaitGroup
	semaphoreChan chan bool
}

func newMove(clusters data.Clusters, synchronize Synchronize, logger *zap.Logger) *move {
	return &move{
		clusters:      clusters,
		synchronize:   synchronize,
		logger:        logger,
		semaphoreWG:   sync.WaitGroup{},
		semaphoreChan: make(chan bool, moveSemaphoreLimit),
	}
}

func (m *move) move(targetDataNode cluster2.DataNode, sourceNodeAddr string, sha512Hex string, bulkError *errors.BulkError) {
	defer func() {
		<-m.semaphoreChan
		m.semaphoreWG.Done()
	}()

	if targetDataNode.SyncMove(sha512Hex, sourceNodeAddr) != nil {
		bulkError.Add(fmt.Errorf("%s, sha512Hex: %s", errors.ErrSync, sha512Hex))
	}
}

func (m *move) complete(sourceClusterId string, targetClusterId string) {
	syncClustersFunc := func(wg *sync.WaitGroup, clusterId string, keepFrozen bool) {
		defer wg.Done()
		if err := m.synchronize.Cluster(clusterId, true, keepFrozen, false); err != nil {
			m.logger.Error(
				"Cluster sync is failed after move operation",
				zap.String("clusterId", clusterId),
				zap.Error(err),
			)
		}
	}
	wg := &sync.WaitGroup{}
	wg.Add(1)
	go syncClustersFunc(wg, sourceClusterId, true)
	wg.Add(1)
	go syncClustersFunc(wg, targetClusterId, false)
	wg.Wait()
}

func (m *move) Move(sourceClusterId string, targetClusterId string) error {
	m.logger.Info(fmt.Sprintf("Cluster moving from %s to %s is started...", sourceClusterId, targetClusterId))

	sourceCluster, err := m.clusters.Get(sourceClusterId)
	if err != nil {
		return err
	}

	if sourceCluster.Used > 0 && sourceCluster.Frozen {
		return errors.ErrNotAvailableForClusterAction
	}

	targetCluster, err := m.clusters.Get(targetClusterId)
	if err != nil {
		return err
	}

	if targetCluster.Used > 0 && targetCluster.Frozen {
		return errors.ErrNotAvailableForClusterAction
	}

	if sourceCluster.Used > targetCluster.Available() {
		return errors.ErrNoSpace
	}

	if err := m.clusters.SetFreeze(sourceClusterId, true); err != nil {
		return err
	}

	if err := m.clusters.SetFreeze(targetClusterId, true); err != nil {
		return err
	}

	sourceMasterNode := sourceCluster.Master()
	smdn, err := cluster2.NewDataNode(sourceMasterNode.Address)
	if err != nil {
		return err
	}

	targetMasterNode := targetCluster.Master()
	tmdn, err := cluster2.NewDataNode(targetMasterNode.Address)
	if err != nil {
		return err
	}

	sourceContainer, err := smdn.SyncList(nil)
	if err != nil {
		m.logger.Error(
			"Unable to get sync list from data node",
			zap.String("nodeId", sourceMasterNode.Id),
			zap.String("nodeAddress", sourceMasterNode.Address),
			zap.Error(err),
		)
		return errors.ErrPing
	}

	for i := len(sourceContainer.Snapshots) - 1; i >= 0; i-- {
		if !smdn.SnapshotDelete(uint64(i)) {
			m.logger.Error(
				"Unable to drop snapshots, it will create problem in future so move process must be failed",
				zap.String("nodeId", sourceMasterNode.Id),
				zap.String("nodeAddress", sourceMasterNode.Address),
			)
			return errors.ErrSnapshot
		}
	}

	bulkErr := errors.NewBulkError()
	for sha512Hex := range sourceContainer.FileItems {
		bulkErrCount := bulkErr.Count()

		if bulkErrCount > 0 && bulkErrCount >= int(float64(len(sourceContainer.FileItems))*moveFailureThreshold) {
			bulkErr.Add(errors.ErrTooManyErrors)
			break
		}

		m.semaphoreChan <- true
		m.semaphoreWG.Add(1)
		go m.move(tmdn, sourceMasterNode.Address, sha512Hex, bulkErr)
	}
	m.semaphoreWG.Wait()

	if bulkErr.HasError() {
		m.logger.Warn("Moving has error(s), please check the following logs to identify the problem")
	}

	m.logger.Info("Moving will be completed after the sync of clusters")
	m.complete(sourceClusterId, targetClusterId)

	if bulkErr.HasError() {
		return bulkErr
	}

	return nil
}
