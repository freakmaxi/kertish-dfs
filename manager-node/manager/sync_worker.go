package manager

const queueLimit = 500
const parallelLimit = 10

type syncWorker struct {
	queueChan       chan *nodeSync
	processSlotChan chan int

	processor *syncProcessor
}

func newChannelContainer() *syncWorker {
	return &syncWorker{
		queueChan:       make(chan *nodeSync, queueLimit),
		processSlotChan: make(chan int, parallelLimit),
		processor:       newSyncProcessor(),
	}
}

func (c *syncWorker) Start() {
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

func (c *syncWorker) Queue(ns *nodeSync) {
	c.queueChan <- ns
}

func (c *syncWorker) process(ns *nodeSync, index int) {
	if !c.processor.Sync(ns) {
		c.processSlotChan <- index
		c.queueChan <- ns
		return
	}
	c.processSlotChan <- index
}
