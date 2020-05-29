package manager

import (
	"sync"

	"github.com/freakmaxi/kertish-dfs/basics/common"
	cluster2 "github.com/freakmaxi/kertish-dfs/manager-node/cluster"
	"go.uber.org/zap"
)

type syncProcessor struct {
	nodeCacheMutex sync.Mutex
	nodeCache      map[string]cluster2.DataNode

	logger *zap.Logger
}

func newSyncProcessor(logger *zap.Logger) *syncProcessor {
	return &syncProcessor{
		nodeCacheMutex: sync.Mutex{},
		nodeCache:      make(map[string]cluster2.DataNode),
		logger:         logger,
	}
}

func (d *syncProcessor) get(node *common.Node) (cluster2.DataNode, error) {
	d.nodeCacheMutex.Lock()
	defer d.nodeCacheMutex.Unlock()

	dn, has := d.nodeCache[node.Id]
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

func (d *syncProcessor) Sync(ns *nodeSync) bool {
	if ns.create {
		d.create(ns.sourceAddr, ns.sha512Hex, ns.targets)
		for i := 0; i < len(ns.targets); i++ {
			target := ns.targets[i]

			if target.completed || target.counter <= 0 {
				if !target.completed {
					d.logger.Error(
						"Sync is failed (CREATE)",
						zap.String("sha512Hex", ns.sha512Hex),
						zap.String("targetNodeId", target.node.Id),
					)
				}
				ns.targets = append(ns.targets[0:i], ns.targets[i+1:]...)
				i--
			}
		}
		return len(ns.targets) == 0
	}

	d.delete(ns.sha512Hex, ns.targets)
	for i := 0; i < len(ns.targets); i++ {
		target := ns.targets[i]

		if target.completed || target.counter <= 0 {
			if !target.completed {
				d.logger.Error(
					"Sync is failed (DELETE)",
					zap.String("sha512Hex", ns.sha512Hex),
					zap.String("targetNodeId", target.node.Id),
				)
			}
			ns.targets = append(ns.targets[0:i], ns.targets[i+1:]...)
			i--
		}
	}
	return len(ns.targets) == 0
}

func (d *syncProcessor) create(sourceAddress string, sha512Hex string, targets []*targetContainer) {
	wg := &sync.WaitGroup{}
	for _, t := range targets {
		wg.Add(1)
		go func(wg *sync.WaitGroup, target *targetContainer) {
			defer wg.Done()

			dn, err := d.get(target.node)
			if err != nil {
				target.counter--
				d.logger.Warn(
					"Data Node Connection Creation is unsuccessful",
					zap.String("targetNodeId", target.node.Id),
					zap.String("targetAddress", target.node.Address),
					zap.Error(err),
				)
				return
			}

			if !dn.SyncCreate(sha512Hex, sourceAddress) {
				target.counter--
				d.logger.Warn(
					"Sync is unsuccessful (CREATE)",
					zap.String("sha512Hex", sha512Hex),
					zap.String("targetNodeId", target.node.Id),
					zap.String("sourceAddress", sourceAddress),
				)
				return
			}

			target.completed = true
		}(wg, t)
	}
	wg.Wait()
}

func (d *syncProcessor) delete(sha512Hex string, targets []*targetContainer) {
	wg := &sync.WaitGroup{}
	for _, t := range targets {
		wg.Add(1)
		go func(wg *sync.WaitGroup, target *targetContainer) {
			defer wg.Done()

			dn, err := d.get(target.node)
			if err != nil {
				target.counter--
				d.logger.Warn(
					"Data Node Connection Creation is unsuccessful",
					zap.String("targetNodeId", target.node.Id),
					zap.String("targetAddress", target.node.Address),
					zap.Error(err),
				)
				return
			}

			if !dn.SyncDelete(sha512Hex) {
				target.counter--
				d.logger.Warn(
					"Sync is unsuccessful (DELETE)",
					zap.String("sha512Hex", sha512Hex),
					zap.String("targetNodeId", target.node.Id),
				)
				return
			}

			target.completed = true
		}(wg, t)
	}
	wg.Wait()
}
