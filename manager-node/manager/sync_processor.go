package manager

import (
	"fmt"
	"sync"

	"github.com/freakmaxi/kertish-dfs/basics/common"
	cluster2 "github.com/freakmaxi/kertish-dfs/manager-node/cluster"
)

type syncProcessor struct {
	nodeCacheMutex sync.Mutex
	nodeCache      map[string]cluster2.DataNode
}

func newSyncProcessor() *syncProcessor {
	return &syncProcessor{
		nodeCacheMutex: sync.Mutex{},
		nodeCache:      make(map[string]cluster2.DataNode),
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
					fmt.Printf("ERROR: Sync is failed: %s <- %s (CREATE)\n", target.node.Id, ns.sha512Hex)
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
				fmt.Printf("ERROR: Sync is failed: %s <- %s (DELETE)\n", target.node.Id, ns.sha512Hex)
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
				fmt.Printf("WARN: Data Node Connection Creation is unsuccessful. nodeId: %s, address: %s - %s\n", target.node.Id, target.node.Address, err.Error())
				return
			}

			if !dn.SyncCreate(sha512Hex, sourceAddress) {
				target.counter--
				fmt.Printf("WARN: Sync is unsuccessful: %s <- %s (CREATE)\n", target.node.Id, sha512Hex)
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
				fmt.Printf("WARN: Data Node Connection Creation is unsuccessful. nodeId: %s, address: %s - %s\n", target.node.Id, target.node.Address, err.Error())
				return
			}

			if !dn.SyncDelete(sha512Hex) {
				target.counter--
				fmt.Printf("WARN: Sync is unsuccessful: %s <- %s (DELETE)\n", target.node.Id, sha512Hex)
				return
			}

			target.completed = true
		}(wg, t)
	}
	wg.Wait()
}
