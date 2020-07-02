package manager

import (
	"time"

	"github.com/freakmaxi/kertish-dfs/manager-node/data"
	"go.uber.org/zap"
)

const queueLimit = 500
const parallelLimit = 10
const pauseDuration = time.Second * 30

type nodeSyncWorker struct {
	queueChan       chan *nodeSync
	processSlotChan chan int

	processor *nodeSyncProcessor
}

func newNodeSyncWorker(clusters data.Clusters, index data.Index, logger *zap.Logger) *nodeSyncWorker {
	return &nodeSyncWorker{
		queueChan:       make(chan *nodeSync, queueLimit),
		processSlotChan: make(chan int, parallelLimit),
		processor:       newNodeSyncProcessor(clusters, index, logger),
	}
}

func (c *nodeSyncWorker) Start() {
	for i := 0; i < cap(c.processSlotChan); i++ {
		c.processSlotChan <- i
	}

	for ns := range c.queueChan {
		i := <-c.processSlotChan
		go c.process(ns, i)
	}
}

func (c *nodeSyncWorker) Queue(ns *nodeSync) {
	c.queueChan <- ns
}

func (c *nodeSyncWorker) process(ns *nodeSync, index int) {
	if c.processor.Sync(ns) {
		c.processSlotChan <- index
		return
	}

	<-time.After(pauseDuration)

	c.processSlotChan <- index
	c.queueChan <- ns
}
