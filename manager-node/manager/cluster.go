package manager

import (
	"fmt"
	"sort"
	"sync"

	"github.com/freakmaxi/kertish-dfs/basics/common"
	"github.com/freakmaxi/kertish-dfs/basics/errors"
	cluster2 "github.com/freakmaxi/kertish-dfs/manager-node/cluster"
	"github.com/freakmaxi/kertish-dfs/manager-node/data"
)

const balanceThreshold = 0.05

type Cluster interface {
	Register(nodeAddresses []string) (*common.Cluster, error)
	RegisterNodesTo(clusterId string, nodeAddresses []string) error

	UnRegisterCluster(clusterId string) error
	UnRegisterNode(nodeId string) error

	GetClusters() (common.Clusters, error)
	GetCluster(clusterId string) (*common.Cluster, error)

	Reserve(size uint64) (*common.ReservationMap, error)
	Commit(reservationId string, clusterMap map[string]uint64) error
	Discard(reservationId string) error

	MoveCluster(sourceClusterId string, targetClusterId string) error
	BalanceClusters(clusterIds []string) error
	UnFreezeClusters(clusterIds []string) error

	Map(sha512HexList []string, mapType common.MapType) (map[string]string, error)
	Find(sha512Hex string, mapType common.MapType) (string, string, error)
}

type cluster struct {
	clusters data.Clusters
	index    data.Index
	health   Health
}

func NewCluster(clusters data.Clusters, index data.Index, health Health) (Cluster, error) {
	return &cluster{
		clusters: clusters,
		index:    index,
		health:   health,
	}, nil
}

func (c *cluster) Register(nodeAddresses []string) (*common.Cluster, error) {
	cluster := common.NewCluster(newClusterId())

	nodes, clusterSize, err := c.prepareNodes(nodeAddresses, 0)
	if err != nil {
		return nil, err
	}
	cluster.Size = clusterSize
	cluster.Nodes = append(cluster.Nodes, nodes...)

	masterAddress := ""
	for i, node := range cluster.Nodes {
		mA := masterAddress

		if i == 0 {
			node.Master = true
			masterAddress = node.Address
		}

		dn, err := cluster2.NewDataNode(node.Address)
		if err != nil {
			return nil, err
		}
		if !dn.Join(cluster.Id, node.Id, mA) {
			return nil, errors.ErrMode
		}
	}

	if err := c.clusters.RegisterCluster(cluster); err != nil {
		return nil, err
	}

	return cluster, nil
}

func (c *cluster) RegisterNodesTo(clusterId string, nodeAddresses []string) error {
	return c.clusters.Save(clusterId, func(cluster *common.Cluster) error {
		masterNode := cluster.Master()

		nodes, _, err := c.prepareNodes(nodeAddresses, cluster.Size)
		if err != nil {
			return err
		}
		cluster.Nodes = append(cluster.Nodes, nodes...)

		for _, node := range nodes {
			dn, err := cluster2.NewDataNode(node.Address)
			if err != nil {
				return err
			}

			if !dn.Join(clusterId, node.Id, masterNode.Address) {
				return errors.ErrJoin
			}
		}

		return nil
	})
}

func (c *cluster) prepareNodes(nodeAddresses []string, clusterSize uint64) (common.NodeList, uint64, error) {
	nodeMap := make(map[string]*common.Node)
	for _, nodeAddress := range nodeAddresses {
		if _, has := nodeMap[nodeAddress]; has {
			return nil, 0, fmt.Errorf("node address entered twice")
		}

		node, err := cluster2.NewDataNode(nodeAddress)
		if err != nil {
			return nil, 0, err
		}

		if node.Ping() == -1 {
			return nil, 0, errors.ErrPing
		}

		size, err := node.Size()
		if err != nil {
			return nil, 0, err
		}

		if clusterSize > 0 && size != clusterSize {
			return nil, 0, fmt.Errorf("inconsistent size between master and slave")
		}
		clusterSize = size

		hardwareId, err := node.HardwareId()
		if err != nil {
			return nil, 0, err
		}

		nodeId := newNodeId(hardwareId, nodeAddress, clusterSize)
		if _, err := c.clusters.ClusterIdOf(nodeId); err == nil || err != errors.ErrNotFound {
			if err == nil {
				err = errors.ErrRegistered
			}
			return nil, 0, err
		}

		nodeMap[nodeAddress] = &common.Node{
			Id:      nodeId,
			Address: nodeAddress,
			Master:  false,
		}
	}

	r := make(common.NodeList, 0)
	for _, v := range nodeMap {
		r = append(r, v)
	}

	return r, clusterSize, nil
}

func (c *cluster) UnRegisterCluster(clusterId string) error {
	return c.clusters.UnRegisterCluster(clusterId, func(cluster *common.Cluster) error {
		if err := c.index.Replace(clusterId, nil); err != nil {
			return err
		}
		for _, node := range cluster.Nodes {
			dn, err := cluster2.NewDataNode(node.Address)
			if err != nil {
				continue
			}
			dn.Wipe()
		}
		return nil
	})
}

func (c *cluster) UnRegisterNode(nodeId string) error {
	return c.clusters.UnRegisterNode(
		nodeId,
		func(cluster *common.Cluster) error {
			return c.health.SyncCluster(cluster, false)
		},
		func(deletingNode *common.Node) error {
			dn, err := cluster2.NewDataNode(deletingNode.Address)
			if err != nil || !dn.Leave() {
				return errors.ErrMode
			}
			return nil
		},
		func(newMaster *common.Node) error {
			dn, err := cluster2.NewDataNode(newMaster.Address)
			if err != nil || !dn.Mode(true) {
				return errors.ErrMode
			}
			return nil
		})
}

func (c *cluster) GetClusters() (common.Clusters, error) {
	return c.clusters.GetAll()
}

func (c *cluster) GetCluster(clusterId string) (*common.Cluster, error) {
	return c.clusters.Get(clusterId)
}

func (c *cluster) Reserve(size uint64) (*common.ReservationMap, error) {
	var reservationMap *common.ReservationMap

	if err := c.clusters.SaveAll(func(clusters common.Clusters) error {
		var err error
		reservationMap, err = c.createReservationMap(size, clusters)

		return err
	}); err != nil {
		return nil, err
	}

	return reservationMap, nil
}

func (c *cluster) Commit(reservationId string, clusterMap map[string]uint64) error {
	return c.clusters.SaveAll(func(clusters common.Clusters) error {
		for _, cluster := range clusters {
			v, has := clusterMap[cluster.Id]
			if !has {
				v = 0
			}
			cluster.Commit(reservationId, v)
		}
		return nil
	})
}

func (c *cluster) Discard(reservationId string) error {
	return c.clusters.SaveAll(func(clusters common.Clusters) error {
		for _, cluster := range clusters {
			cluster.Discard(reservationId)
		}
		return nil
	})
}

func (c *cluster) MoveCluster(sourceClusterId string, targetClusterId string) (e error) {
	sourceCluster, err := c.clusters.Get(sourceClusterId)
	if err != nil {
		return err
	}

	targetCluster, err := c.clusters.Get(targetClusterId)
	if err != nil {
		return err
	}

	if sourceCluster.Used > 0 && sourceCluster.Frozen {
		return errors.ErrNotAvailableForClusterAction
	}

	if err := c.clusters.SetFreeze(sourceClusterId, true); err != nil {
		return err
	}

	if targetCluster.Used > 0 && targetCluster.Frozen {
		return errors.ErrNotAvailableForClusterAction
	}

	if err := c.clusters.SetFreeze(targetClusterId, true); err != nil {
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

	sourceFileItemList := smdn.SyncList()
	if sourceFileItemList == nil {
		return errors.ErrPing
	}

	var syncErr error
	for len(sourceFileItemList) > 0 {
		sourceFileItem := sourceFileItemList[0]

		if !tmdn.SyncMove(sourceFileItem.Sha512Hex, sourceMasterNode.Address) {
			syncErr = errors.ErrSync
		}

		sourceFileItemList = sourceFileItemList[1:]
	}

	syncClustersFunc := func(wg *sync.WaitGroup, cluster *common.Cluster, keepFrozen bool) {
		defer wg.Done()
		_ = c.health.SyncCluster(cluster, keepFrozen)
	}
	wg := &sync.WaitGroup{}
	wg.Add(1)
	go syncClustersFunc(wg, sourceCluster, true)
	wg.Add(1)
	go syncClustersFunc(wg, targetCluster, false)
	wg.Wait()

	return syncErr
}

func (c *cluster) BalanceClusters(clusterIds []string) error {
	clusters, err := c.clusters.GetAll()
	if err != nil {
		return err
	}

	clusterMap := make(map[string]*common.Cluster)
	for _, cluster := range clusters {
		clusterMap[cluster.Id] = cluster
	}

	balancingClusters := make(common.Clusters, 0)
	for len(clusterIds) > 0 {
		clusterId := clusterIds[0]

		cluster, has := clusterMap[clusterId]
		if !has {
			return errors.ErrNotFound
		}

		balancingClusters = append(balancingClusters, cluster)
		clusterIds = clusterIds[1:]
	}

	if len(balancingClusters) == 0 {
		balancingClusters = clusters
	}

	sortFunc := func(fileItemList common.SyncFileItems) common.SyncFileItems {
		sort.Slice(fileItemList, func(i, j int) bool {
			return fileItemList[i].Size < fileItemList[j].Size
		})
		return fileItemList
	}

	indexingMap := make(map[string]common.SyncFileItems)
	for _, cluster := range balancingClusters {
		if cluster.Used > 0 && cluster.Frozen {
			return errors.ErrNotAvailableForClusterAction
		}

		if err := c.clusters.SetFreeze(cluster.Id, true); err != nil {
			return err
		}
		defer func(clusterId string) {
			if err := c.clusters.SetFreeze(clusterId, false); err != nil {
				fmt.Printf("ERROR: Balancing error: unfreezing is failed for %s\n", clusterId)
			}
		}(cluster.Id)

		fileItemList, err := c.index.List(cluster.Id)
		if err != nil {
			return err
		}
		indexingMap[cluster.Id] = sortFunc(fileItemList)
	}

	for {
		sort.Sort(balancingClusters)
		emptiestCluster := balancingClusters[0]
		fullestCluster := balancingClusters[len(balancingClusters)-1]

		if fullestCluster.Weight()-emptiestCluster.Weight() < balanceThreshold {
			break
		}

		sourceFileItemIndex := len(indexingMap[fullestCluster.Id]) / 2
		sourceFileItem := indexingMap[fullestCluster.Id][sourceFileItemIndex]

		tmdn, err := cluster2.NewDataNode(emptiestCluster.Master().Address)
		if err != nil {
			continue
		}

		if !tmdn.SyncMove(sourceFileItem.Sha512Hex, fullestCluster.Master().Address) {
			continue
		}

		emptiestCluster.Used += uint64(sourceFileItem.Size)
		fullestCluster.Used -= uint64(sourceFileItem.Size)

		indexingMap[fullestCluster.Id] =
			append(
				indexingMap[fullestCluster.Id][:sourceFileItemIndex],
				indexingMap[fullestCluster.Id][sourceFileItemIndex+1:]...,
			)

		indexingMap[emptiestCluster.Id] = append(indexingMap[emptiestCluster.Id], sourceFileItem)
		indexingMap[emptiestCluster.Id] = sortFunc(indexingMap[emptiestCluster.Id])
	}

	syncClustersFunc := func(wg *sync.WaitGroup, cluster *common.Cluster, keepFrozen bool) {
		defer wg.Done()
		_ = c.health.SyncCluster(cluster, keepFrozen)
	}
	wg := &sync.WaitGroup{}
	for _, cluster := range balancingClusters {
		wg.Add(1)
		go syncClustersFunc(wg, cluster, true)
	}
	wg.Wait()

	return nil
}

func (c *cluster) UnFreezeClusters(clusterIds []string) error {
	if len(clusterIds) == 0 {
		clusters, err := c.clusters.GetAll()
		if err != nil {
			return err
		}

		for _, cluster := range clusters {
			clusterIds = append(clusterIds, cluster.Id)
		}
	}

	for _, clusterId := range clusterIds {
		if err := c.clusters.SetFreeze(clusterId, false); err != nil {
			return err
		}
	}

	return nil
}

func (c *cluster) Map(sha512HexList []string, mapType common.MapType) (map[string]string, error) {
	clusterMapping := make(map[string]string, 0)
	for _, sha512Hex := range sha512HexList {
		_, address, err := c.Find(sha512Hex, mapType)
		if err != nil {
			if err == errors.ErrNotFound && mapType == common.MT_Delete {
				continue
			}
			return nil, err
		}
		clusterMapping[sha512Hex] = address
	}
	return clusterMapping, nil
}

func (c *cluster) Find(sha512Hex string, mapType common.MapType) (string, string, error) {
	clusters, err := c.clusters.GetAll()
	if err != nil {
		return "", "", err
	}

	clusterIds := make([]string, 0)
	clustersMap := make(map[string]*common.Cluster)
	for _, cluster := range clusters {
		clusterIds = append(clusterIds, cluster.Id)
		clustersMap[cluster.Id] = cluster
	}

	clusterId, _, err := c.index.Find(clusterIds, sha512Hex)
	if err != nil {
		return "", "", err
	}

	cluster := clustersMap[clusterId]
	if cluster.Paralyzed {
		return "", "", errors.ErrNoAvailableClusterNode
	}

	var node *common.Node

	switch mapType {
	case common.MT_Read:
		node = cluster.HighQualityNode()
	default:
		node = cluster.Master()
	}

	if node == nil {
		return "", "", errors.ErrNoAvailableClusterNode
	}

	return clusterId, node.Address, nil
}

var _ Cluster = &cluster{}
