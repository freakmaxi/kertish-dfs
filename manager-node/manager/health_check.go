package manager

import (
	"sort"
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

// has CacheFileItem (cacheExpiresIn) Expire relation
// the value should be strictly less than cacheExpiresIn value
const maintainInterval = time.Hour * (24 * 5)

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
				if err == errors.ErrMaintain {
					h.logger.Warn("Cluster is in maintain mode, skipping...", zap.String("clusterId", cluster.Id))
					continue
				}
				if err == errors.ErrOffline {
					h.logger.Warn("Offline cluster is skipped to maintain", zap.String("clusterId", cluster.Id))
					continue
				}
				if err == errors.ErrParalyzed {
					h.logger.Warn("Paralyzed cluster is skipped to maintain", zap.String("clusterId", cluster.Id))
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
			if cluster.State == common.StateOffline {
				continue
			}
			wg.Add(1)
			go h.checkHealth(wg, cluster)
		}
		wg.Wait()
	}
}

func (h *healthCheck) checkHealth(wg *sync.WaitGroup, cluster *common.Cluster) {
	defer wg.Done()

	if !h.clusterLocking(cluster.Id, true) {
		return
	}
	defer h.clusterLocking(cluster.Id, false)

	cluster.Paralyzed = !h.checkMasterAlive(cluster)

	h.evaluateNodesConnectionQuality(cluster)
	_ = h.clusters.UpdateNodes(cluster)

	if cluster.Maintain {
		return
	}

	var newMaster *common.Node

	if !cluster.Paralyzed {
		newMaster = cluster.HighQualityMasterNodeCandidate()
		if newMaster == nil ||
			!h.ensureMasterNodeCandidateConsistency(cluster.Id, newMaster) {
			return
		}
	} else {
		newMaster = h.findNextMaster(cluster)
		if newMaster == nil {
			h.logger.Error(
				"There is no master node candidate, current master must be up. Cluster will be kept as paralyzed",
				zap.String("clusterId", cluster.Id),
			)
			return
		}
	}

	if err := h.clusters.SetNewMaster(cluster.Id, newMaster.Id); err == nil {
		_ = cluster.SetMaster(newMaster.Id)
		h.notifyNewMasterInCluster(cluster)
	}
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

func (h *healthCheck) findNextMaster(cluster *common.Cluster) *common.Node {
	currentMasterNode := cluster.Master()

	for _, node := range cluster.Nodes {
		if strings.Compare(node.Id, currentMasterNode.Id) == 0 {
			continue
		}
		if !h.ensureMasterNodeCandidateConsistency(cluster.Id, node) {
			continue
		}
		return node
	}
	return nil
}

func (h *healthCheck) ensureMasterNodeCandidateConsistency(clusterId string, node *common.Node) bool {
	dn, err := h.getDataNode(node)
	if err != nil {
		h.logger.Warn(
			"Ensuring the master node candidate consistency is failed",
			zap.String("clusterId", clusterId),
			zap.String("nodeId", node.Id),
			zap.Error(err),
		)
		return false
	}

	pr := dn.Ping()

	if pr == -1 {
		return false
	}

	container, err := dn.SyncList(nil)
	if err != nil {
		return false
	}

	return h.index.CompareMap(clusterId, container.FileItems)
}

func (h *healthCheck) evaluateNodesConnectionQuality(cluster *common.Cluster) {
	qualityDisabled := int64(^uint64(0) >> 1)

	for _, node := range cluster.Nodes {
		dn, err := h.getDataNode(node)
		if err != nil {
			h.logger.Warn(
				"Evaluating the connection quality of the node is failed",
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
	sort.Sort(cluster.Nodes)
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
