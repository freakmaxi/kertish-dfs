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

const healthCheckInterval = time.Second * 10
const maintainInterval = time.Hour * 24

type HealthReport map[string]common.NodeList

type HealthCheck interface {
	Start()
	Report() (HealthReport, error)
}

type healthCheck struct {
	clusters    data.Clusters
	index       data.Index
	synchronize Synchronize
	repair      Repair
	logger      *zap.Logger
	interval    time.Duration

	clusterLockMutex sync.Mutex
	clusterLock      map[string]bool

	nodeCacheMutex sync.Mutex
	nodeCache      map[string]cluster2.DataNode
}

func NewHealthTracker(
	clusters data.Clusters,
	index data.Index,
	synchronize Synchronize,
	repair Repair,
	logger *zap.Logger,
	interval time.Duration,
) HealthCheck {

	if interval == 0 {
		interval = healthCheckInterval
	}

	return &healthCheck{
		clusters:         clusters,
		index:            index,
		synchronize:      synchronize,
		repair:           repair,
		logger:           logger,
		interval:         interval,
		clusterLockMutex: sync.Mutex{},
		clusterLock:      make(map[string]bool),
		nodeCacheMutex:   sync.Mutex{},
		nodeCache:        make(map[string]cluster2.DataNode),
	}
}

func (h *healthCheck) getDataNode(node *common.Node) (cluster2.DataNode, error) {
	h.nodeCacheMutex.Lock()
	defer h.nodeCacheMutex.Unlock()

	dn, has := h.nodeCache[node.Address]
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

func (h *healthCheck) clusterLocking(clusterId string, lock bool) bool {
	h.clusterLockMutex.Lock()
	defer h.clusterLockMutex.Unlock()

	locked, has := h.clusterLock[clusterId]
	if !has {
		h.clusterLock[clusterId] = lock
		return true
	}

	if locked && lock {
		return false
	}

	h.clusterLock[clusterId] = lock
	return true
}

func (h *healthCheck) clusterLocked(clusterId string) bool {
	h.clusterLockMutex.Lock()
	defer h.clusterLockMutex.Unlock()

	locked, has := h.clusterLock[clusterId]
	return has && locked
}

func (h *healthCheck) Start() {
	go h.maintain()
	go h.health()
}

func (h *healthCheck) Report() (HealthReport, error) {
	clusters, err := h.clusters.GetAll()
	if err != nil {
		return nil, err
	}

	report := make(HealthReport)
	for _, cluster := range clusters {
		nodeHealthMap := make(common.NodeList, 0)
		for _, node := range cluster.Nodes {
			dn, err := h.getDataNode(node)
			if err != nil {
				node.Quality = -2
				nodeHealthMap = append(nodeHealthMap, node)
				continue
			}

			pr := dn.Ping()

			node.Quality = pr
			nodeHealthMap = append(nodeHealthMap, node)
		}
		report[cluster.Id] = nodeHealthMap
	}

	return report, nil
}

func (h *healthCheck) maintain() {
	for {
		time.Sleep(maintainInterval)

		if h.repair.Status().Processing {
			h.logger.Warn("Skipping cluster maintain because one repair operation is in action...")
			continue
		}

		h.logger.Info("Maintaining Clusters...")

		clusters, err := h.clusters.GetAll()
		if err != nil {
			h.logger.Error(
				"Unable to get cluster list for maintaining",
				zap.Error(err),
			)
			continue
		}

		for _, cluster := range clusters {
			if h.clusterLocked(cluster.Id) {
				h.logger.Warn("Cluster is locked to prevent maintain, skipping...", zap.String("clusterId", cluster.Id))
				continue
			}

			if err := h.synchronize.Cluster(cluster.Id, false, false, false); err != nil {
				if err == errors.ErrFrozen {
					h.logger.Warn("Frozen cluster is skipped to maintain", zap.String("clusterId", cluster.Id))
					continue
				}

				h.logger.Error(
					"Syncing cluster in maintain is failed",
					zap.String("clusterId", cluster.Id),
					zap.Error(err),
				)
			}
		}
		h.logger.Info("Maintain is completed")
	}
}

func (h *healthCheck) health() {
	for {
		time.Sleep(h.interval)

		clusters, err := h.clusters.GetAll()
		if err != nil {
			h.logger.Error(
				"Unable to get cluster list for health check",
				zap.Error(err),
			)
			continue
		}

		wg := &sync.WaitGroup{}
		for _, cluster := range clusters {
			wg.Add(1)
			go h.checkHealth(wg, cluster)
		}
		wg.Wait()
	}
}

func (h *healthCheck) checkHealth(wg *sync.WaitGroup, cluster *common.Cluster) {
	defer wg.Done()

	if cluster.Frozen {
		h.prioritizeNodesByConnectionQuality(cluster)
		_ = h.clusters.UpdateNodes(cluster)

		return
	}

	if h.checkMasterAlive(cluster) {
		cluster.Paralyzed = false
		h.prioritizeNodesByConnectionQuality(cluster)
		_ = h.clusters.UpdateNodes(cluster)

		return
	}

	cluster.Paralyzed = true
	_ = h.clusters.UpdateNodes(cluster)

	newMaster := h.findNextMasterCandidate(cluster)
	if newMaster == nil {
		return
	}

	if strings.Compare(newMaster.Id, cluster.Master().Id) != 0 {
		if err := h.clusters.SetNewMaster(cluster.Id, newMaster.Id); err == nil {
			_ = cluster.SetMaster(newMaster.Id)
			h.notifyNewMasterInCluster(cluster)
		}
	}

	cluster.Paralyzed = false
	h.prioritizeNodesByConnectionQuality(cluster)
	_ = h.clusters.UpdateNodes(cluster)
}

func (h *healthCheck) checkMasterAlive(cluster *common.Cluster) bool {
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

func (h *healthCheck) findNextMasterCandidate(cluster *common.Cluster) *common.Node {
	if !h.clusterLocking(cluster.Id, true) {
		return nil
	}
	defer h.clusterLocking(cluster.Id, false)

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

		container, err := dn.SyncList(nil)
		if err != nil {
			continue
		}

		if !h.index.CompareMap(cluster.Id, container.FileItems) {
			continue
		}

		return node
	}
	return nil
}

func (h *healthCheck) prioritizeNodesByConnectionQuality(cluster *common.Cluster) {
	qualityDisabled := int64(^uint64(0) >> 1)

	for _, node := range cluster.Nodes {
		dn, err := h.getDataNode(node)
		if err != nil {
			h.logger.Warn(
				"Prioritizing node by connection quality is failed",
				zap.String("clusterId", cluster.Id),
				zap.String("nodeId", node.Id),
				zap.Error(err),
			)

			node.Quality = qualityDisabled
			continue
		}

		pr := dn.Ping()

		if pr == -1 {
			node.Quality = qualityDisabled
			continue
		}

		if node.Quality == qualityDisabled && !dn.RequestHandshake() {
			continue
		}

		node.Quality = pr
	}
}

func (h *healthCheck) notifyNewMasterInCluster(cluster *common.Cluster) {
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

var _ HealthCheck = &healthCheck{}
