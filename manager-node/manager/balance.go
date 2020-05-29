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
	clusters data.Clusters
	index    data.Index
	logger   *zap.Logger
	health   Health

	mapMutex    sync.Mutex
	indexingMap map[string]common.SyncFileItemList

	semaphoreMutex sync.Mutex
	semaphoreChan  map[string]chan bool
}

func newBalance(clusters data.Clusters, index data.Index, logger *zap.Logger, health Health) *balance {
	return &balance{
		clusters:       clusters,
		index:          index,
		logger:         logger,
		health:         health,
		mapMutex:       sync.Mutex{},
		indexingMap:    make(map[string]common.SyncFileItemList),
		semaphoreMutex: sync.Mutex{},
		semaphoreChan:  make(map[string]chan bool),
	}
}

func (b *balance) nextChunk(sourceCluster *common.Cluster) (*common.SyncFileItem, error) {
	b.mapMutex.Lock()
	defer b.mapMutex.Unlock()

	fileItemList := b.indexingMap[sourceCluster.Id]
	if len(fileItemList) == 0 {
		return nil, os.ErrNotExist
	}
	sourceFileItem := fileItemList[0]

	if err := b.index.Remove(sourceCluster.Id, sourceFileItem.Sha512Hex); err != nil {
		return nil, err
	}
	b.indexingMap[sourceCluster.Id] = fileItemList[1:]

	return &sourceFileItem, nil
}

func (b *balance) moved(targetClusterId string, movedFileItem common.SyncFileItem) {
	b.mapMutex.Lock()
	defer b.mapMutex.Unlock()

	b.indexingMap[targetClusterId] = append(b.indexingMap[targetClusterId], movedFileItem)
	// if it fails, let the cluster sync fix it. till that moment, file will be zombie
	_ = b.index.Add(targetClusterId, movedFileItem)
}

func (b *balance) move(sha512Hex string, sourceAddress string, targetAddress string) int {
	tmdn, err := cluster2.NewDataNode(targetAddress)
	if err != nil {
		return -1
	}

	if !tmdn.SyncMove(sha512Hex, sourceAddress) {
		return 0
	}

	return 1
}

func (b *balance) complete(balancingClusters common.Clusters) {
	syncClustersFunc := func(wg *sync.WaitGroup, cluster *common.Cluster) {
		defer wg.Done()
		_ = b.health.SyncCluster(cluster, false)
	}
	wg := &sync.WaitGroup{}
	for _, cluster := range balancingClusters {
		wg.Add(1)
		go syncClustersFunc(wg, cluster)
	}
	wg.Wait()
}

func (b *balance) Balance(clusterIds []string) error {
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

		fileItemList, err := b.index.List(cluster.Id)
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

		sourceFileItem, err := b.nextChunk(fullestCluster)
		if err != nil {
			if err == os.ErrNotExist {
				break
			}
			b.logger.Warn("Balancing failure, unable to get chunk data", zap.Error(err))
			continue
		}

		atomicSizeFunc(emptiestCluster.Id, uint64(sourceFileItem.Size), true)
		atomicSizeFunc(fullestCluster.Id, uint64(sourceFileItem.Size), false)

		<-getSemaphoreFunc(emptiestCluster.Id)

		wg.Add(1)
		go func(wg *sync.WaitGroup, emptiestCluster common.Cluster, fullestCluster common.Cluster, sourceFileItem common.SyncFileItem) {
			defer wg.Done()
			defer func() { getSemaphoreFunc(emptiestCluster.Id) <- true }()

			// -1 = Connectivity Problem, 0 = Unsuccessful Move Operation (Read error or Deleted file), 1 = Successful
			if result := b.move(sourceFileItem.Sha512Hex, fullestCluster.Master().Address, emptiestCluster.Master().Address); result < 1 {
				if result == 0 {
					return
				}

				b.moved(fullestCluster.Id, sourceFileItem)

				atomicSizeFunc(emptiestCluster.Id, uint64(sourceFileItem.Size), false)
				atomicSizeFunc(fullestCluster.Id, uint64(sourceFileItem.Size), true)
			}
			b.moved(emptiestCluster.Id, sourceFileItem)
		}(wg, *emptiestCluster, *fullestCluster, *sourceFileItem)
	}
	wg.Wait()

	b.complete(balancingClusters)

	return nil
}
