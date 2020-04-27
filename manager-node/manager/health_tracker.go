package manager

import (
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/freakmaxi/kertish-dfs/basics/common"
	cluster2 "github.com/freakmaxi/kertish-dfs/manager-node/cluster"
	"github.com/freakmaxi/kertish-dfs/manager-node/data"
)

type HealthTracker interface {
	Start()
}

const defaultIntervalDuration = time.Second * 10

type healthTracker struct {
	clusters         data.Clusters
	index            data.Index
	intervalDuration time.Duration

	nodeCacheMutex sync.Mutex
	nodeCache      map[string]cluster2.DataNode
}

func NewHealthTracker(clusters data.Clusters, index data.Index, intervalDuration time.Duration) HealthTracker {
	if intervalDuration == 0 {
		intervalDuration = defaultIntervalDuration
	}

	return &healthTracker{
		clusters:         clusters,
		index:            index,
		intervalDuration: intervalDuration,
		nodeCacheMutex:   sync.Mutex{},
		nodeCache:        make(map[string]cluster2.DataNode),
	}
}

func (h *healthTracker) Start() {
	go func() {
		for {
			select {
			case <-time.After(h.intervalDuration):
				wg := &sync.WaitGroup{}

				var registeredClusters common.Clusters
				_ = h.clusters.LockAll(func(clusters common.Clusters) error {
					registeredClusters = make(common.Clusters, len(clusters))
					copy(registeredClusters, clusters)

					return nil
				})

				if registeredClusters == nil {
					continue
				}

				for _, cluster := range registeredClusters {
					wg.Add(1)
					go h.checkHealth(wg, cluster)
				}

				wg.Wait()
			}
		}
	}()
}

func (h *healthTracker) checkHealth(wg *sync.WaitGroup, cluster *common.Cluster) {
	defer wg.Done()

	if !h.checkMasterAlive(cluster) {
		newMaster := h.findBestMasterNodeCandidate(cluster)
		if newMaster != nil && strings.Compare(newMaster.Id, cluster.Master().Id) == 0 {
			if err := h.clusters.SetNewMaster(cluster.Id, newMaster.Id); err == nil {
				h.notifyNewMasterInCluster(cluster)
			}
		}
	}
	h.prioritizeNodesByConnectionQuality(cluster)
	_ = h.clusters.UpdateNodes(cluster)
}

func (h *healthTracker) checkMasterAlive(cluster *common.Cluster) bool {
	masterNode := cluster.Master()

	h.nodeCacheMutex.Lock()
	dn, has := h.nodeCache[masterNode.Id]
	h.nodeCacheMutex.Unlock()

	if has {
		return dn.Clone().Ping() > -1
	}

	dn, err := cluster2.NewDataNode(masterNode.Address)
	if err != nil {
		fmt.Printf("ERROR: Master Node Live Check is failed. clusterId: %s, nodeId: %s - %s\n", cluster.Id, masterNode.Id, err.Error())
		return false
	}

	h.nodeCacheMutex.Lock()
	h.nodeCache[masterNode.Id] = dn
	h.nodeCacheMutex.Unlock()

	return dn.Clone().Ping() > -1
}

func (h *healthTracker) findBestMasterNodeCandidate(cluster *common.Cluster) *common.Node {
	for _, node := range cluster.Nodes {
		h.nodeCacheMutex.Lock()
		dn, has := h.nodeCache[node.Id]
		h.nodeCacheMutex.Unlock()

		if !has {
			var err error
			dn, err = cluster2.NewDataNode(node.Address)
			if err != nil {
				fmt.Printf("ERROR: Finding Best Master Node Candidate is failed. clusterId: %s, nodeId: %s - %s\n", cluster.Id, node.Id, err.Error())
				continue
			}

			h.nodeCacheMutex.Lock()
			h.nodeCache[node.Id] = dn
			h.nodeCacheMutex.Unlock()
		}

		dn = dn.Clone()
		pr := dn.Ping()

		if pr == -1 {
			continue
		}

		serverSha512HexList := dn.SyncList()
		if serverSha512HexList == nil {
			continue
		}

		failed, err := h.index.Compare(cluster.Id, serverSha512HexList)
		if err != nil {
			continue
		}

		if failed == 0 {
			return node
		}
	}
	return nil
}

func (h *healthTracker) prioritizeNodesByConnectionQuality(cluster *common.Cluster) {
	for _, node := range cluster.Nodes {
		h.nodeCacheMutex.Lock()
		dn, has := h.nodeCache[node.Id]
		h.nodeCacheMutex.Unlock()

		if !has {
			var err error
			dn, err = cluster2.NewDataNode(node.Address)
			if err != nil {
				fmt.Printf("ERROR: Prioritizing Node Connection Quality is failed. clusterId: %s, nodeId: %s - %s\n", cluster.Id, node.Id, err.Error())

				node.Quality = int64(^uint(0) >> 1)
				continue
			}

			h.nodeCacheMutex.Lock()
			h.nodeCache[node.Id] = dn
			h.nodeCacheMutex.Unlock()
		}

		dn = dn.Clone()
		pr := dn.Ping()

		if pr == -1 {
			node.Quality = int64(^uint(0) >> 1)
			continue
		}
		node.Quality = pr
	}
}

func (h *healthTracker) notifyNewMasterInCluster(cluster *common.Cluster) {
	for _, node := range cluster.Nodes {
		h.nodeCacheMutex.Lock()
		dn, has := h.nodeCache[node.Id]
		h.nodeCacheMutex.Unlock()

		if !has {
			var err error
			dn, err = cluster2.NewDataNode(node.Address)
			if err != nil {
				fmt.Printf("ERROR: Notifing New Master Node is failed. clusterId: %s, nodeId: %s - %s\n", cluster.Id, node.Id, err.Error())
				continue
			}

			h.nodeCacheMutex.Lock()
			h.nodeCache[node.Id] = dn
			h.nodeCacheMutex.Unlock()
		}

		dn = dn.Clone()

		if dn.Ping() == -1 {
			continue
		}
		dn.Mode(node.Master)
	}
}
