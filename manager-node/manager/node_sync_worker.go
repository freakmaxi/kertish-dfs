package manager

import (
	"github.com/freakmaxi/kertish-dfs/manager-node/data"
	"go.uber.org/zap"
)

const queueLimit = 500
const parallelLimit = 10

type nodeSyncWorker struct {
	queueChan       chan *nodeSync
	processSlotChan chan int

	processor *nodeSyncProcessor
}

func newNodeSyncWorker(index data.Index, logger *zap.Logger) *nodeSyncWorker {
	return &nodeSyncWorker{
		queueChan:       make(chan *nodeSync, queueLimit),
		processSlotChan: make(chan int, parallelLimit),
		processor:       newNodeSyncProcessor(index, logger),
	}
}

func (c *nodeSyncWorker) Start() {
	for i := 0; i < cap(c.processSlotChan); i++ {
		c.processSlotChan <- i
	}

	for {
		select {
		case ns, more := <-c.queueChan:
			if !more {
				return
			}

			i := <-c.processSlotChan
			go c.process(ns, i)
		}
	}
}

func (c *nodeSyncWorker) Queue(ns *nodeSync) {
	c.queueChan <- ns
}

func (c *nodeSyncWorker) process(ns *nodeSync, index int) {
	if !c.processor.Sync(ns) {
		c.processSlotChan <- index
		c.queueChan <- ns
		return
	}
	c.processSlotChan <- index
}
