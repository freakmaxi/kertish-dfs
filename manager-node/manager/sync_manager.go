package manager

import "sync"

type syncManager struct {
	workersMutex sync.Mutex
	workers      map[string]*syncWorker
}

func newWorkerManager() *syncManager {
	return &syncManager{
		workersMutex: sync.Mutex{},
		workers:      make(map[string]*syncWorker),
	}
}

func (s *syncManager) Queue(ns *nodeSync) {
	s.workersMutex.Lock()
	defer s.workersMutex.Unlock()

	cc, has := s.workers[ns.clusterId]
	if !has {
		cc = newChannelContainer()
		s.workers[ns.clusterId] = cc
		go cc.Start()
	}

	cc.Queue(ns)
}
