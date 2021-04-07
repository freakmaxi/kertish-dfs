package manager

import (
	"os"
	"sort"
	"sync"

	"github.com/freakmaxi/kertish-dfs/basics/common"
	"github.com/freakmaxi/kertish-dfs/basics/errors"
	cluster2 "github.com/freakmaxi/kertish-dfs/manager-node/cluster"
	"github.com/freakmaxi/kertish-dfs/manager-node/data"
	"go.uber.org/zap"
)

const balanceThreshold = 0.05
const semaphoreLimit = 10

type balance struct {
	clusters    data.Clusters
	index       data.Index
	logger      *zap.Logger
	synchronize Synchronize

	mapMutex    sync.Mutex
	indexingMap map[string]map[string]string

	semaphoreMutex sync.Mutex
	semaphoreChan  map[string]chan bool
}

func newBalance(clusters data.Clusters, index data.Index, synchronize Synchronize, logger *zap.Logger) *balance {
	return &balance{
		clusters:       clusters,
		index:          index,
		synchronize:    synchronize,
		logger:         logger,
		mapMutex:       sync.Mutex{},
		indexingMap:    make(map[string]map[string]string),
		semaphoreMutex: sync.Mutex{},
		semaphoreChan:  make(map[string]chan bool),
	}
}

func (b *balance) nextChunk(sourceCluster *common.Cluster) (*common.CacheFileItem, error) {
	b.mapMutex.Lock()
	defer b.mapMutex.Unlock()

	fileItemMap := b.indexingMap[sourceCluster.Id]
	if len(fileItemMap) == 0 {
		return nil, os.ErrNotExist
	}

	for sha512Hex := range fileItemMap {
		c, err := b.index.Get(sha512Hex)
		if err != nil {
			if err == os.ErrNotExist {
				continue
			}
			return nil, err
		}

		b.index.QueueDrop(sourceCluster.Id, sha512Hex)
		delete(fileItemMap, sha512Hex)

		return c, nil
	}

	return nil, os.ErrNotExist
}

func (b *balance) moved(targetClusterId string, masterNodeId string, movedFileItem common.SyncFileItem) {
	b.mapMutex.Lock()
	defer b.mapMutex.Unlock()

	cacheFileItem := common.NewCacheFileItem(targetClusterId, masterNodeId, movedFileItem)

	b.indexingMap[targetClusterId][movedFileItem.Sha512Hex] = "moved"
	// if it fails, let the cluster sync fix it. till that moment, file will be zombie
	b.index.QueueUpsert(cacheFileItem)
}

func (b *balance) move(sha512Hex string, sourceAddress string, targetAddress string) int {
	tmdn, err := cluster2.NewDataNode(targetAddress)
	if err != nil {
		return -1
	}

	if tmdn.SyncMove(sha512Hex, sourceAddress) != nil {
		return 0
	}

	return 1
}

func (b *balance) complete(balancingClusters common.Clusters) {
	for _, cluster := range balancingClusters {
		b.synchronize.QueueCluster(cluster.Id, true)
	}
}

func (b *balance) Balance(clusterIds []string) error {
	b.logger.Info("Cluster balancing is started...")

	clusters, err := b.clusters.GetAll()
	if err != nil {
		return err
	}

	clusterMap := make(map[string]*common.Cluster)
	for _, cluster := range clusters {
		clusterMap[cluster.Id] = cluster
	}

	balancingClusters := make(common.Clusters, 0)
	for len(clusterIds) > 0 {
		clusterId := clusterIds[0]

		cluster, has := clusterMap[clusterId]
		if !has {
			return errors.ErrNotFound
		}

		balancingClusters = append(balancingClusters, cluster)
		clusterIds = clusterIds[1:]
	}

	if len(balancingClusters) == 0 {
		balancingClusters = clusters
	}

	for _, cluster := range balancingClusters {
		if cluster.Used > 0 && cluster.Frozen {
			return errors.ErrNotAvailableForClusterAction
		}

		fileItemList, err := b.index.PullMap(cluster.Id)
		if err != nil {
			return err
		}
		b.indexingMap[cluster.Id] = fileItemList
	}

	getSemaphoreFunc := func(clusterId string) chan bool {
		b.semaphoreMutex.Lock()
		defer b.semaphoreMutex.Unlock()

		s, has := b.semaphoreChan[clusterId]
		if !has {
			s = make(chan bool, semaphoreLimit)
			for i := 0; i < cap(s); i++ {
				s <- true
			}
			b.semaphoreChan[clusterId] = s
		}

		return s
	}

	atomicSizeMutex := sync.Mutex{}
	atomicSizeFunc := func(clusterId string, size uint64, add bool) {
		atomicSizeMutex.Lock()
		defer atomicSizeMutex.Unlock()

		if add {
			clusterMap[clusterId].Used += size
			return
		}
		clusterMap[clusterId].Used -= size
	}

	wg := &sync.WaitGroup{}
	for {
		sort.Sort(balancingClusters)
		emptiestCluster := balancingClusters[0]
		fullestCluster := balancingClusters[len(balancingClusters)-1]

		if fullestCluster.Weight()-emptiestCluster.Weight() < balanceThreshold {
			break
		}

		sourceCacheFileItem, err := b.nextChunk(fullestCluster)
		if err != nil {
			if err == os.ErrNotExist {
				break
			}
			b.logger.Warn("Balancing failure, unable to get chunk data", zap.Error(err))
			continue
		}

		atomicSizeFunc(emptiestCluster.Id, uint64(sourceCacheFileItem.FileItem.Size), true)
		atomicSizeFunc(fullestCluster.Id, uint64(sourceCacheFileItem.FileItem.Size), false)

		semaphoreChan := getSemaphoreFunc(emptiestCluster.Id)
		<-semaphoreChan

		wg.Add(1)
		go func(wg *sync.WaitGroup, emptiestCluster common.Cluster, fullestCluster common.Cluster, cacheFileItem common.CacheFileItem, semaphoreChan chan bool) {
			defer wg.Done()
			defer func() { semaphoreChan <- true }()

			// -1 = Connectivity Problem, 0 = Unsuccessful Move Operation (Read error or Deleted file), 1 = Successful
			if result := b.move(cacheFileItem.FileItem.Sha512Hex, fullestCluster.Master().Address, emptiestCluster.Master().Address); result < 1 {
				if result == 0 {
					return
				}

				b.moved(fullestCluster.Id, fullestCluster.Master().Id, cacheFileItem.FileItem)

				atomicSizeFunc(emptiestCluster.Id, uint64(cacheFileItem.FileItem.Size), false)
				atomicSizeFunc(fullestCluster.Id, uint64(cacheFileItem.FileItem.Size), true)
			}
			b.moved(emptiestCluster.Id, emptiestCluster.Master().Id, cacheFileItem.FileItem)
		}(wg, *emptiestCluster, *fullestCluster, *sourceCacheFileItem, semaphoreChan)
	}
	wg.Wait()

	b.logger.Info("Balancing will be completed after the sync of clusters")

	b.complete(balancingClusters)

	return nil
}
