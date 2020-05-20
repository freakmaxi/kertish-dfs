package manager

import (
	"fmt"
	"strings"
	"time"

	"github.com/freakmaxi/kertish-dfs/basics/common"
	"github.com/freakmaxi/kertish-dfs/manager-node/data"
)

const retryLimit = 10

type Node interface {
	Handshake(nodeHardwareAddr string, nodeAddress string, size uint64) (string, string, string, error)
	Create(nodeId string, sha512HexList []string) error
	Delete(nodeId string, syncDeleteList common.SyncDeleteList) error
}

type node struct {
	index    data.Index
	clusters data.Clusters

	syncManager *syncManager
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

func NewNode(index data.Index, clusters data.Clusters) Node {
	return &node{
		index:       index,
		clusters:    clusters,
		syncManager: newWorkerManager(),
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

func (n *node) Create(nodeId string, sha512HexList []string) error {
	clusterId, err := n.clusters.ClusterIdOf(nodeId)
	if err != nil {
		return err
	}

	if err := n.index.AddBulk(clusterId, sha512HexList); err != nil {
		return fmt.Errorf("adding to index failed: \n    clusterId: %s\n    sha512HexList: %s\n        error: %s\n", clusterId, strings.Join(sha512HexList, ","), err.Error())
	}

	cluster, err := n.clusters.Get(clusterId)
	if err != nil {
		return fmt.Errorf("getting cluster is failed: \n    clusterId: %s\n    sha512HexList: %s\n        error: %s\n", clusterId, strings.Join(sha512HexList, ","), err.Error())
	}

	sourceNode := cluster.Node(nodeId)
	targetNodes := cluster.Others(nodeId)
	if targetNodes == nil {
		return fmt.Errorf("node id didn't match to get others: %s\n", nodeId)
	}
	if len(targetNodes) == 0 {
		return nil // nothing to sync
	}

	for _, sha512Hex := range sha512HexList {
		n.syncManager.Queue(
			&nodeSync{
				create:     true,
				date:       time.Now().UTC(),
				clusterId:  cluster.Id,
				sourceAddr: sourceNode.Address,
				sha512Hex:  sha512Hex,
				targets:    n.makeTargetContainerList(targetNodes),
			})
	}

	return nil
}

func (n *node) Delete(nodeId string, syncDeleteList common.SyncDeleteList) error {
	clusterId, err := n.clusters.ClusterIdOf(nodeId)
	if err != nil {
		return err
	}

	return n.clusters.Save(clusterId, func(cluster *common.Cluster) error {
		wiped := syncDeleteList.Wiped()
		if err := n.index.RemoveBulk(cluster.Id, wiped); err != nil {
			return fmt.Errorf("removing from index failed: \n    clusterId: %s\n    sha512Hex: %s\n        error: %s\n", clusterId, strings.Join(wiped, ","), err.Error())
		}
		cluster.Used -= syncDeleteList.Size()

		sourceNode := cluster.Node(nodeId)
		targetNodes := cluster.Others(nodeId)
		if targetNodes == nil {
			return fmt.Errorf("node id didn't match to get others: %s\n", nodeId)
		}
		if len(targetNodes) == 0 {
			return nil // nothing to sync
		}

		for _, syncDelete := range syncDeleteList {
			n.syncManager.Queue(
				&nodeSync{
					create:     false,
					date:       time.Now().UTC(),
					clusterId:  cluster.Id,
					sourceAddr: sourceNode.Address,
					sha512Hex:  syncDelete.Sha512Hex,
					targets:    n.makeTargetContainerList(targetNodes),
				})
		}

		return nil
	})
}

var _ Node = &node{}
