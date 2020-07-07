package manager

import (
	"fmt"
	"sync"
	"time"

	"github.com/freakmaxi/kertish-dfs/manager-node/data"
	"go.uber.org/zap"
)

const queueLimit = 500
const parallelLimit = 10
const pauseDuration = time.Second * 30

type nodeSyncWorker struct {
	queueChan     chan string
	semaphoreChan chan bool

	processor *nodeSyncProcessor

	queueMapMutex sync.Mutex
	queueMap      map[string]chan nodeSync

	processingMapMutex sync.Mutex
	processingChanMap  map[string]chan bool
}

func newNodeSyncWorker(clusters data.Clusters, index data.Index, logger *zap.Logger) *nodeSyncWorker {
	return &nodeSyncWorker{
		queueChan:     make(chan string, queueLimit),
		semaphoreChan: make(chan bool, parallelLimit),
		processor:     newNodeSyncProcessor(clusters, index, logger),

		queueMapMutex: sync.Mutex{},
		queueMap:      make(map[string]chan nodeSync),

		processingMapMutex: sync.Mutex{},
		processingChanMap:  make(map[string]chan bool),
	}
}

func (c *nodeSyncWorker) placeAndQueue(ns *nodeSync) {
	c.queueMapMutex.Lock()
	defer c.queueMapMutex.Unlock()

	ch, has := c.queueMap[ns.sha512Hex]
	if !has {
		c.queueChan <- ns.sha512Hex

		ch = make(chan nodeSync, queueLimit)
		c.queueMap[ns.sha512Hex] = ch
	}
	ch <- *ns
}

func (c *nodeSyncWorker) displaceFromMap(sha512Hex string) <-chan nodeSync {
	c.queueMapMutex.Lock()
	defer c.queueMapMutex.Unlock()

	ns := c.queueMap[sha512Hex]
	delete(c.queueMap, sha512Hex)

	close(ns)

	return ns
}

func (c *nodeSyncWorker) processingChan(sha512Hex string) chan bool {
	c.processingMapMutex.Lock()
	defer c.processingMapMutex.Unlock()

	ch, has := c.processingChanMap[sha512Hex]
	if !has {
		ch = make(chan bool, 1)
		c.processingChanMap[sha512Hex] = ch
	}

	return ch
}

func (c *nodeSyncWorker) process(sha512Hex string, nsCh <-chan nodeSync) {
	c.processingChan(sha512Hex) <- true
	defer func() {
		c.semaphoreChan <- true
		<-c.processingChan(sha512Hex)
	}()

	for ns := range nsCh {
		fmt.Printf("%+v\n", ns)

		for !c.processor.Sync(&ns) {
			time.Sleep(pauseDuration)
		}
	}
}

func (c *nodeSyncWorker) Start() {
	for i := 0; i < cap(c.semaphoreChan); i++ {
		c.semaphoreChan <- true
	}

	for sha512Hex := range c.queueChan {
		nsCh := c.displaceFromMap(sha512Hex)

		<-c.semaphoreChan
		go c.process(sha512Hex, nsCh)
	}
}

func (c *nodeSyncWorker) Queue(ns *nodeSync) {
	c.placeAndQueue(ns)
}
