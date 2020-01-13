package manager

import (
	"crypto/sha512"
	"encoding/hex"
	"fmt"
	"io"
	"sync"

	cluster2 "github.com/freakmaxi/2020-dfs/head-node/src/cluster"
	"github.com/freakmaxi/2020-dfs/head-node/src/common"
	"github.com/freakmaxi/2020-dfs/head-node/src/errors"
)

type create struct {
	reservation *common.Reservation
	failed      bool

	chunks     []dataState
	chunksLock sync.Mutex

	clusterUsage map[string]uint64
}

type dataState struct {
	*common.DataChunk
	address string
}

func NewCreate(reservation *common.Reservation) *create {
	return &create{
		reservation:  reservation,
		failed:       false,
		chunks:       make([]dataState, 0),
		chunksLock:   sync.Mutex{},
		clusterUsage: make(map[string]uint64),
	}
}

func (c *create) calculateHash(data []byte) string {
	hash := sha512.New512_256()
	hash.Write(data)
	return hex.EncodeToString(hash.Sum(nil))
}

func (c *create) process(reader io.Reader, findClusterHandler func(sha512Hex string) (string, string, error)) (common.DataChunks, map[string]uint64, error) {
	wg := &sync.WaitGroup{}
	for _, clusterMap := range c.reservation.Clusters {
		if c.failed {
			break
		}

		buffer := make([]byte, clusterMap.Chunk.Size)
		_, err := io.ReadAtLeast(reader, buffer, len(buffer))
		if err != nil {
			return nil, nil, err
		}

		wg.Add(1)
		go c.upload(wg, clusterMap, buffer, findClusterHandler)
	}
	wg.Wait()

	if c.failed {
		c.revert()
		return nil, nil, fmt.Errorf("create is failed, check logs for details")
	}

	chunks := make(common.DataChunks, 0)
	for _, chunk := range c.chunks {
		chunks = append(chunks, chunk.DataChunk)
	}

	return chunks, c.clusterUsage, nil
}

func (c *create) upload(wg *sync.WaitGroup, clusterMap common.ClusterMap, data []byte, findClusterHandler func(sha512Hex string) (string, string, error)) {
	defer wg.Done()

	sha512Hex := c.calculateHash(data)
	clusterId, address, err := findClusterHandler(sha512Hex)
	if err != nil {
		if err != errors.ErrNoAvailableNode {
			c.failed = true

			fmt.Printf("ERROR: Find Cluster is failed. index: %d, clusterId: %s -  %s\n", clusterMap.Chunk.Starts(), clusterMap.Id, err.Error())
			return
		}
		clusterId = clusterMap.Id
		address = clusterMap.Address
	}

	exists, sha512Hex, err := cluster2.NewDataNode(address).Create(data)
	if err != nil {
		c.failed = true

		fmt.Printf("ERROR: Create on Cluster is failed. index: %d, clusterId: %s -  %s\n", clusterMap.Chunk.Starts(), clusterMap.Id, err.Error())
		return
	}

	c.addChunk(clusterId, address, clusterMap.Chunk.Sequence, uint32(len(data)), sha512Hex, exists)
}

func (c *create) addChunk(clusterId string, address string, sequence uint16, size uint32, sha512Hex string, exists bool) {
	c.chunksLock.Lock()
	defer c.chunksLock.Unlock()

	if !exists {
		if _, has := c.clusterUsage[clusterId]; !has {
			c.clusterUsage[clusterId] = 0
		}
		c.clusterUsage[clusterId] += uint64(size)
	}

	c.chunks = append(c.chunks, dataState{DataChunk: common.NewDataChunk(sequence, size, sha512Hex), address: address})
}

func (c *create) revert() {
	for len(c.chunks) > 0 {
		chunk := c.chunks[0]
		if err := cluster2.NewDataNode(chunk.address).Delete(chunk.Hash); err != nil {
			fmt.Printf("ERROR: Revert create is failed. address: %s -  %s\n", chunk.address, err.Error())

			c.chunks = append(c.chunks[1:], chunk)
			continue
		}
		c.chunks = c.chunks[1:]
	}
}
