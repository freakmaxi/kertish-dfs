package manager

import (
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/freakmaxi/kertish-dfs/basics/common"
	"github.com/freakmaxi/kertish-dfs/basics/errors"
	cluster2 "github.com/freakmaxi/kertish-dfs/manager-node/cluster"
	"github.com/freakmaxi/kertish-dfs/manager-node/data"
	"go.uber.org/zap"
)

const balanceThreshold = 0.05
const balanceSemaphoreLimit = 10

type balance struct {
	clusters    data.Clusters
	index       data.Index
	synchronize Synchronize
	logger      *zap.Logger

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

func (b *balance) Balance(clusterIds []string) error {
	b.logger.Info("Cluster balancing is started...")

	clusterIdsMap := make(map[string]bool)
	for _, clusterId := range clusterIds {
		clusterIdsMap[clusterId] = true
	}

	clusters, err := b.clusters.GetAll()
	if err != nil {
		return err
	}

	clusterMap := make(map[string]*common.Cluster)
	balancingClusters := make(common.Clusters, 0)
	for _, cluster := range clusters {
		if len(clusterIdsMap) == 0 {
			clusterMap[cluster.Id] = cluster
			balancingClusters = append(balancingClusters, cluster)
			continue
		}
		if _, has := clusterIdsMap[cluster.Id]; has {
			clusterMap[cluster.Id] = cluster
			balancingClusters = append(balancingClusters, cluster)
		}
	}

	for _, cluster := range clusterMap {
		if cluster.Maintain {
			return errors.ErrMaintain
		}
		if err := b.clusters.UpdateMaintain(cluster.Id, true, common.TopicBalance); err != nil {
			return err
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
			s = make(chan bool, balanceSemaphoreLimit)
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
			defer func() {
				semaphoreChan <- true
				wg.Done()
			}()

			if err := b.move(cacheFileItem.FileItem.Sha512Hex, fullestCluster.Master().Address, emptiestCluster.Master().Address); err != nil {
				b.logger.Warn("Failed to move the file chunk between clusters", zap.Error(err))

				b.returnChunk(fullestCluster.Id, cacheFileItem.FileItem.Sha512Hex)

				atomicSizeFunc(emptiestCluster.Id, uint64(cacheFileItem.FileItem.Size), false)
				atomicSizeFunc(fullestCluster.Id, uint64(cacheFileItem.FileItem.Size), true)
				return
			}
			b.moved(emptiestCluster.Id, emptiestCluster.Master().Id, fullestCluster.Id, cacheFileItem.FileItem)
		}(wg, *emptiestCluster, *fullestCluster, *sourceCacheFileItem, semaphoreChan)
	}
	wg.Wait()

	b.logger.Info("Balancing is completed")

	b.complete(balancingClusters)

	return nil
}

func (b *balance) nextChunk(sourceCluster *common.Cluster) (*common.CacheFileItem, error) {
	b.mapMutex.Lock()
	defer b.mapMutex.Unlock()

	fileItemMap := b.indexingMap[sourceCluster.Id]
	if len(fileItemMap) == 0 {
		return nil, os.ErrNotExist
	}

	for sha512Hex, state := range fileItemMap {
		// Do not move back the moved file chunk
		if strings.Compare(state, "moved") == 0 {
			continue
		}

		c, err := b.index.Get(sha512Hex)
		if err != nil {
			if err == os.ErrNotExist {
				continue
			}
			return nil, err
		}

		delete(fileItemMap, sha512Hex)

		return c, nil
	}

	return nil, os.ErrNotExist
}

func (b *balance) returnChunk(sourceClusterId string, sha512Hex string) {
	b.mapMutex.Lock()
	defer b.mapMutex.Unlock()

	b.indexingMap[sourceClusterId][sha512Hex] = "returned"
}

func (b *balance) move(sha512Hex string, sourceAddress string, targetAddress string) error {
	tmdn, err := cluster2.NewDataNode(targetAddress)
	if err != nil {
		return err
	}

	if err := tmdn.SyncMove(sha512Hex, sourceAddress); err != nil {
		return err
	}

	return nil
}

func (b *balance) moved(targetClusterId string, targetMasterNodeId string, sourceClusterId string, movedFileItem common.SyncFileItem) {
	b.mapMutex.Lock()
	defer b.mapMutex.Unlock()

	b.indexingMap[targetClusterId][movedFileItem.Sha512Hex] = "moved"

	syncTime := time.Now().UTC()
	b.index.QueueUpsert(common.NewCacheFileItem(targetClusterId, targetMasterNodeId, movedFileItem), &syncTime)
	b.index.QueueDrop(sourceClusterId, movedFileItem.Sha512Hex)
}

func (b *balance) complete(balancingClusters common.Clusters) {
	for _, cluster := range balancingClusters {
		b.synchronize.QueueCluster(cluster.Id, true, false)
	}
}
