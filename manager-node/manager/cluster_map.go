package manager

import (
	"fmt"

	"github.com/freakmaxi/kertish-dfs/basics/common"
	"github.com/freakmaxi/kertish-dfs/basics/errors"
	cluster2 "github.com/freakmaxi/kertish-dfs/manager-node/cluster"
	"github.com/google/uuid"
)

func (c *cluster) createMap(size uint64, clusters common.Clusters) (*common.ReservationMap, error) {
	chunks := c.calculateChunks(size)
	reservationId := uuid.New().String()

	i1, i2 := 0, 0
	r := make([]common.ClusterMap, 0)
	for i1 < len(chunks) {
		chunk := chunks[i1]
		nextGroupIdx := i2 % len(clusters)
		cluster := clusters[nextGroupIdx]

		nm, err := c.makeNodeMap(chunk, cluster)
		if err != nil {
			clusters = append(clusters[:nextGroupIdx], clusters[nextGroupIdx+1:]...)
			if len(clusters) == 0 {
				if err != errors.ErrNoDiskSpace {
					return nil, fmt.Errorf("reservation is not possible")
				}
				return nil, err
			}

			i2++
			continue
		}
		cluster.Reserve(reservationId, uint64(chunk.Size))

		r = append(r, *nm)
		i1++
		i2++
	}

	return &common.ReservationMap{
		Id:       reservationId,
		Clusters: r,
	}, nil
}

func (c *cluster) calculateChunks(size uint64) []common.Chunk {
	if size < uint64(blockSize) {
		return []common.Chunk{{Index: 0, Size: uint32(size)}}
	}

	chunks := make([]common.Chunk, 0)
	idx := uint64(0)
	for seq := uint16(0); idx < size; seq++ {
		chunkSize := blockSize
		if (size - idx) < uint64(chunkSize) {
			chunkSize = uint32(size - idx)
		}
		chunks = append(chunks, common.Chunk{Sequence: seq, Index: idx, Size: chunkSize})
		idx += uint64(chunkSize)
	}

	return chunks
}

func (c *cluster) makeNodeMap(chunk common.Chunk, cluster *common.Cluster) (*common.ClusterMap, error) {
	if cluster.Available() < uint64(chunk.Size) {
		return nil, errors.ErrNoDiskSpace
	}

	masterNode := cluster.Master()
	dn, _ := cluster2.NewDataNode(masterNode.Address)

	if dn.Ping() > -1 {
		return &common.ClusterMap{Id: cluster.Id, Address: masterNode.Address, Chunk: chunk}, nil
	}

	newMaster := c.findBestMasterNodeCandidate(cluster)
	if newMaster != nil {
		if !newMaster.Master {
			if err := cluster.SetMaster(newMaster.Id); err != nil {
				return nil, err
			}
		}
		return &common.ClusterMap{Id: cluster.Id, Address: newMaster.Address, Chunk: chunk}, nil
	}

	return nil, fmt.Errorf("no suitable node to map")
}

func (c *cluster) findBestMasterNodeCandidate(cluster *common.Cluster) *common.Node {
	var selectedNode *common.Node

	leastFailure := uint64(50) // Threshold is 50 sha512Hex
	for _, node := range cluster.Nodes {
		dn, _ := cluster2.NewDataNode(node.Address)
		pr := dn.Ping()

		if pr == -1 {
			continue
		}

		serverSha512HexList := dn.SyncList()
		if serverSha512HexList == nil {
			continue
		}

		failed, err := c.index.Compare(cluster.Id, serverSha512HexList)
		if err != nil {
			continue
		}

		if failed == 0 {
			return node
		}

		if failed < leastFailure {
			leastFailure = failed
			selectedNode = node
		}
	}

	if selectedNode != nil {
		return selectedNode
	}
	return nil
}

func (c *cluster) chooseMostResponsiveNode(cluster *common.Cluster, mapType common.MapType) *common.Node {
	var selectedNode *common.Node

	if mapType != common.MT_Read {
		return cluster.Master()
	}

	pingQuality := int64(1000) // Threshold is 1 second
	for _, node := range cluster.Nodes {
		dn, _ := cluster2.NewDataNode(node.Address)
		pr := dn.Ping()

		if pr == -1 {
			continue
		}

		if pr < pingQuality {
			pingQuality = pr
			selectedNode = node
		}
	}

	if selectedNode != nil {
		return selectedNode
	}
	return nil
}
