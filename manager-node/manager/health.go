package manager

import (
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/freakmaxi/kertish-dfs/basics/common"
	"github.com/freakmaxi/kertish-dfs/basics/errors"
	cluster2 "github.com/freakmaxi/kertish-dfs/manager-node/cluster"
	"github.com/freakmaxi/kertish-dfs/manager-node/data"
)

const defaultIntervalDuration = time.Second * 10
const maintainDuration = time.Hour * 24

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
}

type health struct {
	clusters         data.Clusters
	index            data.Index
	metadata         data.Metadata
	intervalDuration time.Duration

	nodeCacheMutex sync.Mutex
	nodeCache      map[string]cluster2.DataNode
}

func NewHealthTracker(clusters data.Clusters, index data.Index, metadata data.Metadata, intervalDuration time.Duration) Health {
	if intervalDuration == 0 {
		intervalDuration = defaultIntervalDuration
	}

	return &health{
		clusters:         clusters,
		index:            index,
		metadata:         metadata,
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
			fmt.Print("INFO: Maintaining Clusters...\n")
			// Fire Forget
			go func() {
				clusters, err := h.clusters.GetAll()
				if err != nil {
					return
				}

				for _, cluster := range clusters {
					if cluster.Frozen {
						fmt.Printf("WARN: Frozen cluster is skipped to maintain. clusterId: %s\n", cluster.Id)
						continue
					}

					// Do not block all clusters and finished it one by one.
					if err := h.SyncCluster(cluster, false); err != nil {
						fmt.Printf("ERROR: Syncing cluster in Maintain is failed. clusterId: %s - %s\n", cluster.Id, err.Error())
					}
				}
				fmt.Print("INFO: Maintain is completed!\n")
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
		fmt.Printf("ERROR: Master Node Live Check is failed. clusterId: %s, nodeId: %s - %s\n", cluster.Id, masterNode.Id, err.Error())
		return false
	}

	return dn.Ping() > -1
}

func (h *health) findBestMasterNodeCandidate(cluster *common.Cluster) *common.Node {
	for _, node := range cluster.Nodes {
		dn, err := h.getDataNode(node)
		if err != nil {
			fmt.Printf("ERROR: Finding Best Master Node Candidate is failed. clusterId: %s, nodeId: %s - %s\n", cluster.Id, node.Id, err.Error())
			continue
		}

		pr := dn.Ping()

		if pr == -1 {
			continue
		}

		serverFileItemList := dn.SyncList()
		if serverFileItemList == nil {
			continue
		}

		failed, err := h.index.Compare(cluster.Id, serverFileItemList)
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
			fmt.Printf("ERROR: Prioritizing Node Connection Quality is failed. clusterId: %s, nodeId: %s - %s\n", cluster.Id, node.Id, err.Error())

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
			fmt.Printf("ERROR: Notifing New Master Node is failed. clusterId: %s, nodeId: %s - %s\n", cluster.Id, node.Id, err.Error())
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
			fmt.Printf("ERROR: Syncing error: unfreezing is failed for %s\n", cluster.Id)
		}
	}()

	masterNode := cluster.Master()
	slaveNodes := cluster.Slaves()

	mdn, err := cluster2.NewDataNode(masterNode.Address)
	if err != nil || !mdn.Join(cluster.Id, masterNode.Id, "") {
		return errors.ErrJoin
	}

	cluster.Reservations = make(map[string]uint64)
	cluster.Used, _ = mdn.Used()

	if len(slaveNodes) == 0 {
		return nil
	}

	fileItemList := mdn.SyncList()
	if fileItemList == nil {
		fmt.Printf("ERROR: Syncing error: node (%s) didn't response for SyncList\n", masterNode.Id)
		return errors.ErrPing
	}

	if err := h.index.Replace(cluster.Id, fileItemList); err != nil {
		fmt.Printf("ERROR: Index replacement error: %s\n", err.Error())
		return errors.ErrPing
	}

	wg := &sync.WaitGroup{}
	for _, slaveNode := range slaveNodes {
		wg.Add(1)
		go func(wg *sync.WaitGroup, mN *common.Node, sN *common.Node) {
			defer wg.Done()

			sdn, err := cluster2.NewDataNode(sN.Address)
			if err != nil || !sdn.Join(cluster.Id, sN.Id, masterNode.Address) {
				fmt.Printf("ERROR: Syncing error: %s\n", errors.ErrJoin.Error())
				return
			}

			if !sdn.SyncFull(mN.Address) {
				fmt.Printf("ERROR: Syncing node (%s) is failed. Source: %s\n", sN.Id, mN.Address)
			}
		}(wg, masterNode, slaveNode)
	}
	wg.Wait()

	return nil
}

func (h *health) RepairConsistency(repairType RepairType) error {
	repairStructure := repairType == RT_Full || repairType == RT_Structure
	repairIntegrity := repairType == RT_Full || repairType == RT_Integrity

	if repairStructure {
		// Repair Structure
		if err := h.repairStructure(); err != nil {
			return err
		}
	}

	if !repairIntegrity {
		return nil
	}

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

			missingChunkHashes := make([]string, 0)
			for _, chunk := range file.Chunks {
				clusterId, fileItem, err := h.index.Find(clusterIds, chunk.Hash)
				if err != nil {
					if err != errors.ErrNotFound {
						return false, err
					}
					missingChunkHashes = append(missingChunkHashes, chunk.Hash)
					continue
				}

				if uint32(fileItem.Size) != chunk.Size {
					missingChunkHashes = append(missingChunkHashes, chunk.Hash)
					continue
				}

				matchedFileItemListMap[clusterId] = append(matchedFileItemListMap[clusterId], *fileItem)
			}
			if len(missingChunkHashes) == 0 {
				continue
			}

			file.Ingest([]string{}, missingChunkHashes)
			changed = true
		}
		return changed, nil
	}); err != nil {
		return err
	}

	// Make Zombie File Chunk Cleanup
	for clusterId, matchedFileItemList := range matchedFileItemListMap {
		zombieFileItemList, err := h.index.Extract(clusterId, matchedFileItemList)
		if err != nil {
			return err
		}

		masterNode := clusterMap[clusterId].Master()
		mdn, err := cluster2.NewDataNode(masterNode.Address)
		if err != nil {
			return err
		}

		for _, zombieFileItem := range zombieFileItemList {
			if err := mdn.Delete(zombieFileItem.Sha512Hex); err != nil {
				return err
			}
		}
	}

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
