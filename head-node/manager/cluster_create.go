package manager

import (
	"crypto/sha512"
	"encoding/hex"
	"fmt"
	"io"
	"sync"

	"github.com/freakmaxi/kertish-dfs/basics/common"
	"github.com/freakmaxi/kertish-dfs/basics/errors"
	cluster2 "github.com/freakmaxi/kertish-dfs/head-node/cluster"
	"go.uber.org/zap"
)

type create struct {
	reservationMap          *common.ReservationMap
	dataNodeProviderHandler func(address string) (cluster2.DataNode, error)
	findClusterHandler      func(sha512Hex string) (string, string, error)
	logger                  *zap.Logger

	clusterUsageMutex sync.Mutex
	clusterUsage      map[string]uint64

	chunks common.DataChunks
	err    *errors.BulkError
}

func NewCreate(
	reservationMap *common.ReservationMap,
	dataNodeProviderHandler func(address string) (cluster2.DataNode, error),
	findClusterHandler func(sha512Hex string) (string, string, error),
	logger *zap.Logger,
) *create {
	return &create{
		reservationMap:          reservationMap,
		dataNodeProviderHandler: dataNodeProviderHandler,
		findClusterHandler:      findClusterHandler,
		logger:                  logger,
		clusterUsageMutex:       sync.Mutex{},
		clusterUsage:            make(map[string]uint64),
		chunks:                  make(common.DataChunks, 0),
		err:                     errors.NewBulkError(),
	}
}

func (c *create) calculateHash(data []byte) string {
	hash := sha512.New512_256()
	_, _ = hash.Write(data)
	return hex.EncodeToString(hash.Sum(nil))
}

func (c *create) process(reader io.Reader) (*common.CreationResult, map[string]uint64, error) {
	sha512Hash := sha512.New512_256()

	successChan, errorChan, resultWg := c.resultCollectors()

	wg := &sync.WaitGroup{}
	for _, clusterMap := range c.reservationMap.Clusters {
		if c.err.HasError() {
			break
		}

		buffer := make([]byte, clusterMap.Chunk.Size)
		_, err := io.ReadAtLeast(reader, buffer, len(buffer))
		if err != nil {
			errorChan <- err
			break
		}

		_, err = sha512Hash.Write(buffer)
		if err != nil {
			errorChan <- err
			break
		}

		wg.Add(1)
		go c.upload(wg, clusterMap, buffer, successChan, errorChan)
	}
	wg.Wait()

	close(successChan)

	reverted := false
	if c.err.HasError() {
		reverted = c.revert(errorChan)
	}
	close(errorChan)
	resultWg.Wait()

	if c.err.HasError() {
		if c.err.ContainsType(&errors.UploadError{}) &&
			len(c.reservationMap.Clusters) > 1 && // has simultaneous upload
			len(c.chunks) > 0 && // has already uploaded chunk
			len(c.reservationMap.Clusters) != len(c.chunks) && // placement count is not matching
			!reverted {
			c.err.Add(fmt.Errorf("possible zombie file or orphan chunk is appeared. repair may require"))
		}
		return nil, nil, c.err
	}

	sha512Hex := hex.EncodeToString(sha512Hash.Sum(nil))

	return common.NewCreationResult(sha512Hex, c.chunks), c.clusterUsage, nil
}

func (c *create) resultCollectors() (chan *common.DataChunk, chan error, *sync.WaitGroup) {
	wg := &sync.WaitGroup{}

	successChan := make(chan *common.DataChunk)
	errorChan := make(chan error)

	wg.Add(1)
	go func(wg *sync.WaitGroup) {
		defer wg.Done()
		for dataChunk := range successChan {
			c.chunks = append(c.chunks, dataChunk)
		}
	}(wg)

	wg.Add(1)
	go func(wg *sync.WaitGroup) {
		defer wg.Done()
		for err := range errorChan {
			c.err.Add(err)
		}
	}(wg)

	return successChan, errorChan, wg
}

func (c *create) upload(wg *sync.WaitGroup, clusterMap common.ClusterMap, data []byte, successChan chan *common.DataChunk, errorChan chan error) {
	defer wg.Done()

	sha512Hex := c.calculateHash(data)
	clusterId, address, err := c.findClusterHandler(sha512Hex)
	if err != nil {
		if err == errors.ErrRemote {
			errorChan <- errors.NewUploadError(
				fmt.Sprintf(
					"finding cluster communication problem, index: %d, clusterId: %s, error: %s",
					clusterMap.Chunk.Starts(),
					clusterMap.Id,
					err,
				),
			)
			return
		}

		if err == errors.ErrNoAvailableClusterNode {
			errorChan <- errors.NewUploadError(
				fmt.Sprintf(
					"cluster is found for %s but does not have available node to create shadow",
					sha512Hex,
				),
			)
			return
		}

		// Does not find any entry
		clusterId = clusterMap.Id
		address = clusterMap.Address
	}

	dn, err := c.dataNodeProviderHandler(address)
	if err != nil {
		errorChan <- errors.NewUploadError(
			fmt.Sprintf(
				"unable to get data node for creation, index: %d, clusterId: %s, address: %s, error: %s",
				clusterMap.Chunk.Starts(),
				clusterMap.Id,
				address,
				err,
			),
		)
		return
	}

	exists, sha512Hex, err := dn.Create(data)
	if err != nil {
		if errors.IsDialError(err) {
			errorChan <- errors.NewUploadError(
				fmt.Sprintf(
					"unable to create chunk, failure on data node, clusterId: %s, address: %s, error: %s",
					clusterMap.Id,
					address,
					err,
				),
			)
			return
		}

		errorChan <- errors.NewUploadError(
			fmt.Sprintf(
				"unable to create chunk, failure on data node, clusterId: %s, address: %s, sha512Hex: %s, error: %s",
				clusterMap.Id,
				address,
				sha512Hex,
				err,
			),
		)
		return
	}

	clusterUsage := uint32(len(data))
	if exists {
		clusterUsage = 0
	}
	c.updateClusterUsage(clusterId, uint64(clusterUsage))

	successChan <- common.NewDataChunk(clusterMap.Chunk.Sequence, uint32(len(data)), sha512Hex)
}

func (c *create) updateClusterUsage(clusterId string, size uint64) {
	c.clusterUsageMutex.Lock()
	defer c.clusterUsageMutex.Unlock()

	if _, has := c.clusterUsage[clusterId]; !has {
		c.clusterUsage[clusterId] = 0
	}
	c.clusterUsage[clusterId] += size
}

func (c *create) revert(errorChan chan error) bool {
	reverted := true

	for _, dataChunk := range c.chunks {
		clusterId, address, err := c.findClusterHandler(dataChunk.Hash)
		if err != nil {
			reverted = false
			errorChan <- fmt.Errorf(
				"unable to revert chunk creation, sha512Hex: %s, error: %s",
				dataChunk.Hash,
				err,
			)
			continue
		}

		dn, err := c.dataNodeProviderHandler(address)
		if err != nil {
			reverted = false
			errorChan <- fmt.Errorf(
				"unable to get data node for creation reversion, clusterId: %s, address: %s, sha512Hex: %s, error: %s",
				clusterId,
				address,
				dataChunk.Hash,
				err,
			)
			continue
		}

		if err := dn.Delete(dataChunk.Hash); err != nil {
			reverted = false
			errorChan <- fmt.Errorf(
				"unable to delete chunk, failure on data node, clusterId: %s, address: %s, sha512Hex: %s, error: %s",
				clusterId,
				address,
				dataChunk.Hash,
				err,
			)
		}
	}

	return reverted
}
