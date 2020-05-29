package manager

import (
	"sync"

	"go.uber.org/zap"
)

type syncManager struct {
	workersMutex sync.Mutex
	workers      map[string]*syncWorker

	logger *zap.Logger
}

func newWorkerManager(logger *zap.Logger) *syncManager {
	return &syncManager{
		workersMutex: sync.Mutex{},
		workers:      make(map[string]*syncWorker),
		logger:       logger,
	}
}

func (s *syncManager) Queue(ns *nodeSync) {
	s.workersMutex.Lock()
	defer s.workersMutex.Unlock()

	cc, has := s.workers[ns.clusterId]
	if !has {
		cc = newChannelContainer(s.logger)
		s.workers[ns.clusterId] = cc
		go cc.Start()
	}

	cc.Queue(ns)
}
