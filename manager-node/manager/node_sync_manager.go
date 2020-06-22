package manager

import (
	"sync"

	"github.com/freakmaxi/kertish-dfs/manager-node/data"
	"go.uber.org/zap"
)

type nodeSyncManager struct {
	workersMutex sync.Mutex
	workers      map[string]*nodeSyncWorker

	index  data.Index
	logger *zap.Logger
}

func newNodeSyncManager(index data.Index, logger *zap.Logger) *nodeSyncManager {
	return &nodeSyncManager{
		workersMutex: sync.Mutex{},
		workers:      make(map[string]*nodeSyncWorker),
		index:        index,
		logger:       logger,
	}
}

func (s *nodeSyncManager) QueueMany(nss []*nodeSync) {
	if len(nss) == 0 {
		return
	}

	s.workersMutex.Lock()
	defer s.workersMutex.Unlock()

	for _, ns := range nss {
		cc, has := s.workers[ns.clusterId]
		if !has {
			cc = newNodeSyncWorker(s.index, s.logger)
			s.workers[ns.clusterId] = cc
			go cc.Start()
		}

		cc.Queue(ns)
	}
}

func (s *nodeSyncManager) QueueOne(ns *nodeSync) {
	s.workersMutex.Lock()
	defer s.workersMutex.Unlock()

	cc, has := s.workers[ns.clusterId]
	if !has {
		cc = newNodeSyncWorker(s.index, s.logger)
		s.workers[ns.clusterId] = cc
		go cc.Start()
	}

	cc.Queue(ns)
}
