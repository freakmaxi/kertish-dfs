package manager

import (
	"fmt"
	"sync"
	"time"

	"github.com/freakmaxi/kertish-dfs/basics/common"
	cluster2 "github.com/freakmaxi/kertish-dfs/manager-node/cluster"
	"github.com/freakmaxi/kertish-dfs/manager-node/data"
)

const queueLimit = 500
const semaphoreLimit = 10
const syncGapLimit = 100 // millisecond
const retryLimit = 10

type Node interface {
	Handshake(nodeHardwareAddr string, nodeAddress string, size uint64) (string, string, string, error)
	Create(nodeId string, sha512Hex string) error
	Delete(nodeId string, sha512Hex string, shadow bool, size uint64) error
}

type node struct {
	index    data.Index
	clusters data.Clusters

	syncChan chan nodeSync

	semaphoreLock sync.Mutex
	semaphores    map[string]chan bool
}

type nodeSync struct {
	create bool
	date   time.Time

	clusterId  string
	sourceAddr string
	sha512Hex  string
	targets    common.NodeList
	counters   map[string]int
}

func NewNode(index data.Index, clusters data.Clusters) (Node, error) {
	n := &node{
		index:      index,
		clusters:   clusters,
		syncChan:   make(chan nodeSync, queueLimit),
		semaphores: make(map[string]chan bool),
	}
	go n.start()

	return n, nil
}

func (n *node) start() {
	for {
		select {
		case c, m := <-n.syncChan:
			if !m {
				return
			}

			if c.create && time.Now().UTC().Sub(c.date).Milliseconds() < syncGapLimit {
				// this is just created, give some time to master to complete the creation
				n.syncChan <- c
				continue
			}
			go n.processSync(c)
		}
	}
}

func (n *node) processSync(ns nodeSync) {
	if _, has := n.semaphores[ns.clusterId]; !has {
		n.semaphoreLock.Lock()
		n.semaphores[ns.clusterId] = make(chan bool, semaphoreLimit)
		n.semaphoreLock.Unlock()
	}

	n.semaphores[ns.clusterId] <- true
	defer func() {
		<-n.semaphores[ns.clusterId]
	}()

	failed := make(common.NodeList, 0)
	failedLock := sync.Mutex{}
	addFailedFunc := func(failedNode *common.Node) {
		failedLock.Lock()
		defer failedLock.Unlock()

		failed = append(failed, failedNode)
	}

	wg := &sync.WaitGroup{}
	for _, node := range ns.targets {
		if ns.counters[node.Id] <= 0 {
			if ns.create {
				fmt.Printf("ERROR: Sync is failed: %s <- %s (CREATE)\n", node.Id, ns.sha512Hex)
			} else {
				fmt.Printf("ERROR: Sync is failed: %s <- %s (DELETE)\n", node.Id, ns.sha512Hex)
			}
			continue
		}

		wg.Add(1)
		go func(wg *sync.WaitGroup, wn *common.Node) {
			defer wg.Done()
			dn, _ := cluster2.NewDataNode(wn.Address)

			if ns.create {
				if !dn.SyncCreate(ns.sourceAddr, ns.sha512Hex) {
					ns.counters[wn.Id]--
					fmt.Printf("WARN: Sync is failed, will try again: %s <- %s (CREATE)\n", wn.Id, ns.sha512Hex)
					addFailedFunc(wn)
					return
				}
				delete(ns.counters, wn.Id)
				return
			}

			if !dn.SyncDelete(ns.sha512Hex) {
				ns.counters[wn.Id]--
				fmt.Printf("WARN: Sync is failed, will try again: %s <- %s (DELETE)\n", wn.Id, ns.sha512Hex)
				addFailedFunc(wn)
				return
			}
			delete(ns.counters, wn.Id)
		}(wg, node)
	}
	wg.Wait()

	if len(failed) > 0 {
		go func(cns nodeSync, fnl common.NodeList) {
			<-time.After(time.Second * 10)
			n.syncChan <- nodeSync{
				create:     cns.create,
				date:       cns.date,
				clusterId:  cns.clusterId,
				sourceAddr: cns.sourceAddr,
				sha512Hex:  cns.sha512Hex,
				targets:    fnl,
				counters:   cns.counters,
			}
		}(ns, failed)
	}
}

func (n *node) Handshake(nodeHardwareAddr string, nodeAddress string, size uint64) (string, string, string, error) {
	nodeId := newNodeId(nodeHardwareAddr, nodeAddress, size)

	clusterId, err := n.clusters.ClusterIdOf(nodeId)
	if err != nil {
		return "", "", "", err
	}

	var cluster *common.Cluster
	if err := n.clusters.Lock(*clusterId, func(c *common.Cluster) error {
		cluster = c
		return nil
	}); err != nil {
		return "", "", "", err
	}

	syncSourceAddrBind := ""
	node := cluster.Node(nodeId)
	if !node.Master {
		syncSourceAddrBind = cluster.Master().Address
	}

	return cluster.Id, node.Id, syncSourceAddrBind, nil
}

func (n *node) Create(nodeId string, sha512Hex string) error {
	clusterId, err := n.clusters.ClusterIdOf(nodeId)
	if err != nil {
		return err
	}

	return n.clusters.Lock(*clusterId, func(cluster *common.Cluster) error {
		if err := n.index.Add(*clusterId, sha512Hex); err != nil {
			return fmt.Errorf("adding to index failed: \n    clusterId: %s\n    sha512Hex: %s\n        error: %s\n", *clusterId, sha512Hex, err.Error())
		}

		node := cluster.Node(nodeId)
		others := cluster.Others(nodeId)
		if others == nil {
			return fmt.Errorf("node id didn't match to get others: %s\n", nodeId)
		}
		if len(others) == 0 {
			return nil // nothing to sync
		}

		counters := make(map[string]int)
		for _, n := range others {
			counters[n.Id] = retryLimit
		}

		n.syncChan <- nodeSync{
			create:     true,
			date:       time.Now().UTC(),
			clusterId:  cluster.Id,
			sourceAddr: node.Address,
			sha512Hex:  sha512Hex,
			targets:    others,
			counters:   counters,
		}
		return nil
	})
}

func (n *node) Delete(nodeId string, sha512Hex string, shadow bool, size uint64) error {
	clusterId, err := n.clusters.ClusterIdOf(nodeId)
	if err != nil {
		return err
	}

	return n.clusters.Save(*clusterId, func(cluster *common.Cluster) error {
		if !shadow {
			if err := n.index.Remove(cluster.Id, sha512Hex); err != nil {
				return fmt.Errorf("removing from index failed: \n    clusterId: %s\n    sha512Hex: %s\n        error: %s\n", *clusterId, sha512Hex, err.Error())
			}
		}
		cluster.Used -= size

		node := cluster.Node(nodeId)
		others := cluster.Others(nodeId)
		if others == nil {
			return fmt.Errorf("node id didn't match to get others: %s\n", nodeId)
		}
		if len(others) == 0 {
			return nil // nothing to sync
		}

		counters := make(map[string]int)
		for _, n := range others {
			counters[n.Id] = retryLimit
		}

		n.syncChan <- nodeSync{
			create:     false,
			date:       time.Now().UTC(),
			clusterId:  cluster.Id,
			sourceAddr: node.Address,
			sha512Hex:  sha512Hex,
			targets:    others,
			counters:   counters,
		}

		return nil
	})
}

var _ Node = &node{}
