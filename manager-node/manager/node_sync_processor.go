package manager

import (
	"fmt"
	"sync"

	"github.com/freakmaxi/kertish-dfs/basics/common"
	cluster2 "github.com/freakmaxi/kertish-dfs/manager-node/cluster"
	"github.com/freakmaxi/kertish-dfs/manager-node/data"
	"go.uber.org/zap"
)

type nodeSyncProcessor struct {
	nodeCacheMutex sync.Mutex
	nodeCache      map[string]cluster2.DataNode

	clusters data.Clusters
	index    data.Index
	logger   *zap.Logger
}

func newNodeSyncProcessor(clusters data.Clusters, index data.Index, logger *zap.Logger) *nodeSyncProcessor {
	return &nodeSyncProcessor{
		nodeCacheMutex: sync.Mutex{},
		nodeCache:      make(map[string]cluster2.DataNode),
		clusters:       clusters,
		index:          index,
		logger:         logger,
	}
}

func (d *nodeSyncProcessor) get(node *common.Node) (cluster2.DataNode, error) {
	d.nodeCacheMutex.Lock()
	defer d.nodeCacheMutex.Unlock()

	dn, has := d.nodeCache[node.Address]
	if !has {
		var err error
		dn, err = cluster2.NewDataNode(node.Address)
		if err != nil {
			return nil, err
		}
		d.nodeCache[node.Address] = dn
	}

	return dn, nil
}

func (d *nodeSyncProcessor) Sync(ns *nodeSync) bool {
	cluster, err := d.clusters.Get(ns.clusterId)
	if err != nil {
		d.logger.Error(
			"Sync is failed (CLUSTER)",
			zap.String("sha512Hex", ns.sha512Hex),
			zap.Bool("create", ns.create),
			zap.Error(err),
		)
		return false
	}

	if cluster.Paralyzed {
		d.logger.Warn(
			"Sync will try again (PARALYSED)",
			zap.String("sha512Hex", ns.sha512Hex),
			zap.Bool("create", ns.create),
		)
		return false
	}

	syncType := "DELETE"
	if ns.create {
		syncType = "CREATE"
		d.create(ns)
	} else {
		d.delete(ns)
	}

	for i := 0; i < len(ns.targets); i++ {
		target := ns.targets[i]

		if target.completed {
			ns.targets = append(ns.targets[0:i], ns.targets[i+1:]...)
			i--

			continue
		}

		if target.counter > 0 {
			continue
		}

		d.logger.Error(
			fmt.Sprintf("Sync is failed (%s)", syncType),
			zap.String("sha512Hex", ns.sha512Hex),
			zap.String("targetNodeId", target.node.Id),
		)
		ns.targets = append(ns.targets[:i], ns.targets[i+1:]...)
		i--
	}
	return len(ns.targets) == 0
}

func (d *nodeSyncProcessor) create(ns *nodeSync) {
	wg := &sync.WaitGroup{}
	for i := range ns.targets {
		wg.Add(1)
		go func(wg *sync.WaitGroup, target *targetContainer) {
			defer wg.Done()

			target.counter--

			dn, err := d.get(target.node)
			if err != nil {
				d.logger.Warn(
					"Data node connection creation is unsuccessful",
					zap.String("targetNodeId", target.node.Id),
					zap.String("targetAddress", target.node.Address),
					zap.Error(err),
				)
				return
			}

			if err := dn.SyncCreate(ns.sha512Hex, ns.sourceAddr); err != nil {
				d.logger.Warn(
					"Sync is unsuccessful (CREATE)",
					zap.String("sha512Hex", ns.sha512Hex),
					zap.String("targetNodeId", target.node.Id),
					zap.String("sourceAddress", ns.sourceAddr),
					zap.Error(err),
				)
				return
			}

			if err := d.index.UpdateChunkNode(ns.sha512Hex, target.node.Id, true); err != nil {
				d.logger.Warn(
					"Adding node information to the index is failed",
					zap.String("sha512Hex", ns.sha512Hex),
					zap.String("targetNodeId", target.node.Id),
					zap.String("sourceAddress", ns.sourceAddr),
					zap.Error(err),
				)
				return
			}

			target.completed = true
		}(wg, ns.targets[i])
	}
	wg.Wait()
}

func (d *nodeSyncProcessor) delete(ns *nodeSync) {
	wg := &sync.WaitGroup{}
	for i := range ns.targets {
		wg.Add(1)
		go func(wg *sync.WaitGroup, target *targetContainer) {
			defer wg.Done()

			target.counter--

			dn, err := d.get(target.node)
			if err != nil {
				d.logger.Warn(
					"Data node connection creation is unsuccessful",
					zap.String("targetNodeId", target.node.Id),
					zap.String("targetAddress", target.node.Address),
					zap.Error(err),
				)
				return
			}

			if err := dn.SyncDelete(ns.sha512Hex); err != nil {
				d.logger.Warn(
					"Sync is unsuccessful (DELETE)",
					zap.String("sha512Hex", ns.sha512Hex),
					zap.String("targetNodeId", target.node.Id),
					zap.Error(err),
				)
				return
			}

			target.completed = true
		}(wg, ns.targets[i])
	}
	wg.Wait()
}
