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

const moveFailureThreshold = 0.001
const moveSemaphoreLimit = 10

type move struct {
	clusters    data.Clusters
	index       data.Index
	synchronize Synchronize
	logger      *zap.Logger

	semaphoreWG   sync.WaitGroup
	semaphoreChan chan bool
}

func newMove(clusters data.Clusters, index data.Index, synchronize Synchronize, logger *zap.Logger) *move {
	return &move{
		clusters:      clusters,
		index:         index,
		synchronize:   synchronize,
		logger:        logger,
		semaphoreWG:   sync.WaitGroup{},
		semaphoreChan: make(chan bool, moveSemaphoreLimit),
	}
}

func (m *move) Move(sourceClusterId string, targetClusterId string) error {
	m.logger.Info(fmt.Sprintf("Cluster moving from %s to %s is started...", sourceClusterId, targetClusterId))

	sourceCluster, err := m.clusters.Get(sourceClusterId)
	if err != nil {
		return err
	}

	if !sourceCluster.CanSchedule() {
		return errors.ErrNotAvailableForClusterAction
	}
	if err := m.clusters.UpdateStateWithMaintain(sourceCluster.Id, common.StateReadonly, true, common.TopicMove); err != nil {
		return err
	}

	targetCluster, err := m.clusters.Get(targetClusterId)
	if err != nil {
		return err
	}

	if !targetCluster.CanSchedule() {
		return errors.ErrNotAvailableForClusterAction
	}
	if err := m.clusters.UpdateStateWithMaintain(targetCluster.Id, common.StateReadonly, true, common.TopicMove); err != nil {
		return err
	}

	if sourceCluster.Used > targetCluster.Available() {
		return errors.ErrNoSpace
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

	m.logger.Info(fmt.Sprintf("Fetching source (%s) sync list for cluster moving...", sourceCluster.Id))

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

	m.logger.Info("Cluster moving operation is taking place...")

	bulkErr := errors.NewBulkError()
	errorThreshold := int(float64(len(sourceContainer.FileItems)) * moveFailureThreshold)
	for _, fileItem := range sourceContainer.FileItems {
		bulkErrCount := bulkErr.Count()

		if bulkErrCount > 0 && bulkErrCount >= errorThreshold {
			bulkErr.Add(errors.ErrTooManyErrors)
			break
		}

		m.semaphoreChan <- true
		m.semaphoreWG.Add(1)
		go m.move(targetCluster.Id, targetMasterNode.Id, tmdn, sourceCluster.Id, sourceMasterNode.Address, fileItem, bulkErr)
	}
	m.semaphoreWG.Wait()
	m.index.WaitQueueCompletion()

	if bulkErr.HasError() {
		m.logger.Warn("Moving has error(s), please check the following logs to identify the problem")
	}

	m.logger.Info("Moving will be completed after the sync of clusters")

	// Sync operation will remove maintain lock
	if err := m.clusters.UpdateState(sourceCluster.Id, common.StateOffline); err != nil {
		return err
	}
	if err := m.clusters.UpdateState(targetCluster.Id, common.StateOnline); err != nil {
		return err
	}

	m.complete(sourceCluster.Id, targetCluster.Id)

	if bulkErr.HasError() {
		return bulkErr
	}

	return nil
}

func (m *move) move(
	targetClusterId string,
	targetMasterNodeId string,
	targetDataNode cluster2.DataNode,
	sourceClusterId string,
	sourceNodeAddr string,
	fileItem common.SyncFileItem,
	bulkError *errors.BulkError) {
	defer func() {
		<-m.semaphoreChan
		m.semaphoreWG.Done()
	}()

	if targetDataNode.SyncMove(fileItem.Sha512Hex, sourceNodeAddr) != nil {
		bulkError.Add(fmt.Errorf("%s, sha512Hex: %s", errors.ErrSync, fileItem.Sha512Hex))
		return
	}

	syncTime := time.Now().UTC()
	m.index.QueueUpsert(common.NewCacheFileItem(targetClusterId, targetMasterNodeId, fileItem), &syncTime)
	m.index.QueueDrop(sourceClusterId, fileItem.Sha512Hex)
}

func (m *move) complete(sourceClusterId string, targetClusterId string) {
	syncClustersFunc := func(wg *sync.WaitGroup, clusterId string) {
		defer wg.Done()
		if err := m.synchronize.Cluster(clusterId, true, false, false); err != nil {
			m.logger.Error(
				"Cluster sync is failed after move operation",
				zap.String("clusterId", clusterId),
				zap.Error(err),
			)
		}
	}
	wg := &sync.WaitGroup{}
	wg.Add(1)
	go syncClustersFunc(wg, sourceClusterId)
	wg.Add(1)
	go syncClustersFunc(wg, targetClusterId)
	wg.Wait()
}
