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
	}
}

func (c *create) calculateHash(data []byte) string {
	hash := sha512.New512_256()
	hash.Write(data)
	return hex.EncodeToString(hash.Sum(nil))
}

func (c *create) process(reader io.Reader) (common.DataChunks, map[string]uint64, error) {
	successChan := make(chan *common.DataChunk, len(c.reservationMap.Clusters))
	errorChan := make(chan error, len(c.reservationMap.Clusters))

	wg := &sync.WaitGroup{}
	for _, clusterMap := range c.reservationMap.Clusters {
		if len(errorChan) > 0 {
			break
		}

		buffer := make([]byte, clusterMap.Chunk.Size)
		_, err := io.ReadAtLeast(reader, buffer, len(buffer))
		if err != nil {
			errorChan <- err
			break
		}

		wg.Add(1)
		go c.upload(wg, clusterMap, buffer, successChan, errorChan)
	}
	wg.Wait()

	close(successChan)

	if len(errorChan) > 0 {
		c.revert(successChan, errorChan)
		close(errorChan)

		return nil, nil, c.createBulkError(errorChan)
	}
	close(errorChan)

	return c.complete(successChan), c.clusterUsage, nil
}

func (c *create) upload(wg *sync.WaitGroup, clusterMap common.ClusterMap, data []byte, successChan chan *common.DataChunk, errorChan chan error) {
	defer wg.Done()

	sha512Hex := c.calculateHash(data)
	clusterId, address, err := c.findClusterHandler(sha512Hex)
	if err != nil {
		if err == errors.ErrRemote {
			errorChan <- fmt.Errorf(
				"finding cluster communication problem, index: %d, clusterId: %s, error: %s",
				clusterMap.Chunk.Starts(),
				clusterMap.Id,
				err,
			)
			return
		}

		if err == errors.ErrNoAvailableClusterNode {
			errorChan <- fmt.Errorf(
				"cluster is found for %s but does not have available node to create shadow",
				sha512Hex,
			)
			return
		}

		// Does not find any entry
		clusterId = clusterMap.Id
		address = clusterMap.Address
	}

	dn, err := c.dataNodeProviderHandler(address)
	if err != nil {
		errorChan <- fmt.Errorf(
			"unable to get data node for creation, index: %d, clusterId: %s, address: %s, error: %s",
			clusterMap.Chunk.Starts(),
			clusterMap.Id,
			address,
			err,
		)
		return
	}

	exists, sha512Hex, err := dn.Create(data)
	if err != nil {
		errorChan <- fmt.Errorf(
			"unable to create chunk, failure on data node, clusterId: %s, address: %s, sha512Hex: %s, error: %s",
			clusterMap.Id,
			address,
			sha512Hex,
			err,
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

func (c *create) complete(successChan chan *common.DataChunk) common.DataChunks {
	chunks := make(common.DataChunks, 0)
	for {
		select {
		case dataChunk, more := <-successChan:
			if !more {
				return chunks
			}
			chunks = append(chunks, dataChunk)
		}
	}
}

func (c *create) revert(successChan chan *common.DataChunk, errorChan chan error) {
	for {
		select {
		case dataChunk, more := <-successChan:
			if !more {
				return
			}

			clusterId, address, err := c.findClusterHandler(dataChunk.Hash)
			if err != nil {
				errorChan <- fmt.Errorf(
					"unable to revert chunk creation, sha512Hex: %s, error: %s",
					dataChunk.Hash,
					err,
				)
				continue
			}

			dn, err := c.dataNodeProviderHandler(address)
			if err != nil {
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
				errorChan <- fmt.Errorf(
					"unable to delete chunk, failure on data node, clusterId: %s, address: %s, sha512Hex: %s, error: %s",
					clusterId,
					address,
					dataChunk.Hash,
					err,
				)
			}
		}
	}
}

func (c *create) createBulkError(errorChan chan error) error {
	bulkError := errors.NewBulkError()

	for {
		select {
		case err, more := <-errorChan:
			if !more {
				bulkError.Add(fmt.Errorf("possible zombie file or orphan chunk is appeared. repair may require"))
				return bulkError
			}
			bulkError.Add(err)
		}
	}
}
