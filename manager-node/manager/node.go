package manager

import (
	"fmt"
	"time"

	"github.com/freakmaxi/kertish-dfs/basics/common"
	"github.com/freakmaxi/kertish-dfs/manager-node/data"
	"go.uber.org/zap"
)

const retryLimit = 10

type Node interface {
	Handshake(nodeHardwareAddr string, nodeAddress string, size uint64) (string, string, string, error)
	Notify(nodeId string, notificationContainerList common.NotificationContainerList) error
}

type node struct {
	index    data.Index
	clusters data.Clusters

	nodeSyncManager *nodeSyncManager
}

type targetContainer struct {
	node      *common.Node
	counter   int
	completed bool
}

type nodeSync struct {
	create bool
	date   time.Time

	clusterId  string
	sourceAddr string
	sha512Hex  string
	targets    []*targetContainer
}

func NewNode(clusters data.Clusters, index data.Index, logger *zap.Logger) Node {
	return &node{
		index:           index,
		clusters:        clusters,
		nodeSyncManager: newNodeSyncManager(clusters, index, logger),
	}
}

func (n *node) makeTargetContainerList(nodes common.NodeList) []*targetContainer {
	targetContainers := make([]*targetContainer, 0)
	for _, node := range nodes {
		targetContainers = append(targetContainers, &targetContainer{
			node:      node,
			counter:   retryLimit,
			completed: false,
		})
	}
	return targetContainers
}

func (n *node) Handshake(nodeHardwareAddr string, nodeAddress string, size uint64) (string, string, string, error) {
	nodeId := newNodeId(nodeHardwareAddr, nodeAddress, size)

	clusterId, err := n.clusters.ClusterIdOf(nodeId)
	if err != nil {
		return "", "", "", err
	}

	cluster, err := n.clusters.Get(clusterId)
	if err != nil {
		return "", "", "", err
	}

	syncSourceAddrBind := ""
	node := cluster.Node(nodeId)
	if !node.Master {
		syncSourceAddrBind = cluster.Master().Address
	}

	return cluster.Id, node.Id, syncSourceAddrBind, nil
}

func (n *node) Notify(nodeId string, notificationContainerList common.NotificationContainerList) error {
	creatingNotificationContainerList := make(common.NotificationContainerList, 0)
	deletingNotificationContainerList := make(common.NotificationContainerList, 0)

	for len(notificationContainerList) > 0 {
		notificationContainer := notificationContainerList[0]

		if notificationContainer.Create {
			if len(deletingNotificationContainerList) > 0 {
				if err := n.delete(nodeId, deletingNotificationContainerList.ExportFileItemList()); err != nil {
					deletingNotificationContainerList = append(deletingNotificationContainerList, notificationContainerList...)
					return common.NewNotificationError(deletingNotificationContainerList, err)
				}
				deletingNotificationContainerList = make(common.NotificationContainerList, 0)
			}

			creatingNotificationContainerList = append(creatingNotificationContainerList, notificationContainer)
			notificationContainerList = notificationContainerList[1:]
			continue
		}

		if len(creatingNotificationContainerList) > 0 {
			if err := n.create(nodeId, creatingNotificationContainerList.ExportFileItemList()); err != nil {
				creatingNotificationContainerList = append(creatingNotificationContainerList, notificationContainerList...)
				return common.NewNotificationError(creatingNotificationContainerList, err)
			}
			creatingNotificationContainerList = make(common.NotificationContainerList, 0)
		}

		deletingNotificationContainerList = append(deletingNotificationContainerList, notificationContainer)
		notificationContainerList = notificationContainerList[1:]
	}

	if len(creatingNotificationContainerList) > 0 {
		if err := n.create(nodeId, creatingNotificationContainerList.ExportFileItemList()); err != nil {
			return common.NewNotificationError(creatingNotificationContainerList, err)
		}
	}

	if len(deletingNotificationContainerList) > 0 {
		if err := n.delete(nodeId, deletingNotificationContainerList.ExportFileItemList()); err != nil {
			return common.NewNotificationError(deletingNotificationContainerList, err)
		}
	}

	return nil
}

func (n *node) create(nodeId string, fileItemList common.SyncFileItemList) error {
	clusterId, err := n.clusters.ClusterIdOf(nodeId)
	if err != nil {
		return err
	}

	cluster, err := n.clusters.Get(clusterId)
	if err != nil {
		return fmt.Errorf("getting cluster is failed. clusterId: %s, error: %s", clusterId, err)
	}

	sourceNode := cluster.Node(nodeId)
	targetNodes := cluster.Others(nodeId)
	if targetNodes == nil {
		return fmt.Errorf("node id didn't match to get others: %s", nodeId)
	}

	cacheFileItems := make(common.CacheFileItemMap)
	nodeSyncItems := make([]*nodeSync, 0)

	for _, fileItem := range fileItemList {
		cacheFileItems[fileItem.Sha512Hex] = common.NewCacheFileItem(clusterId, nodeId, fileItem)

		if len(targetNodes) == 0 {
			continue
		}

		nodeSyncItems = append(nodeSyncItems, &nodeSync{
			create:     true,
			date:       time.Now().UTC(),
			clusterId:  cluster.Id,
			sourceAddr: sourceNode.Address,
			sha512Hex:  fileItem.Sha512Hex,
			targets:    n.makeTargetContainerList(targetNodes),
		})
	}

	if err := n.index.ReplaceBulk(cacheFileItems); err != nil {
		return fmt.Errorf("adding to index failed. clusterId: %s, error: %s", clusterId, err)
	}

	n.nodeSyncManager.QueueMany(nodeSyncItems)

	return nil
}

func (n *node) delete(nodeId string, fileItemList common.SyncFileItemList) error {
	clusterId, err := n.clusters.ClusterIdOf(nodeId)
	if err != nil {
		return err
	}

	return n.clusters.Save(clusterId, func(cluster *common.Cluster) error {
		sourceNode := cluster.Node(nodeId)
		targetNodes := cluster.Others(nodeId)
		if targetNodes == nil {
			return fmt.Errorf("node id didn't match to get others: %s\n", nodeId)
		}

		if err := n.index.UpdateUsageInMap(cluster.Id, fileItemList.ShadowItems()); err != nil {
			return fmt.Errorf("updating index failed: error: %s", err)
		}

		if err := n.index.DropBulk(cluster.Id, fileItemList.PhysicalFiles()); err != nil {
			return fmt.Errorf("removing from index failed: error: %s", err)
		}
		cluster.Used -= fileItemList.PhysicalSize()

		if len(targetNodes) == 0 {
			return nil // nothing to sync
		}

		for _, fileItem := range fileItemList {
			n.nodeSyncManager.QueueOne(
				&nodeSync{
					create:     false,
					date:       time.Now().UTC(),
					clusterId:  cluster.Id,
					sourceAddr: sourceNode.Address,
					sha512Hex:  fileItem.Sha512Hex,
					targets:    n.makeTargetContainerList(targetNodes),
				})
		}

		return nil
	})
}

var _ Node = &node{}
