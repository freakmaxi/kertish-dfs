package manager

import (
	"sort"

	"github.com/freakmaxi/kertish-dfs/basics/common"
	"github.com/freakmaxi/kertish-dfs/basics/errors"
	"github.com/google/uuid"
)

const blockSize uint32 = 1024 * 1024 * 32 // 32Mb

func (c *cluster) createReservationMap(size uint64, clusters common.Clusters) (*common.ReservationMap, error) {
	chunks := c.calculateChunks(size)
	reservationId := uuid.New().String()

	r := make([]common.ClusterMap, 0)
	for len(chunks) > 0 {
		chunk := chunks[0]

		sort.Sort(clusters)
		cluster := clusters[0]

		if cluster.Paralyzed {
			chunks = chunks[1:]
			continue
		}

		if cluster.Available() < uint64(chunk.Size) {
			return nil, errors.ErrNoDiskSpace
		}

		r = append(r, common.ClusterMap{
			Id:      cluster.Id,
			Address: cluster.Master().Address,
			Chunk:   chunk,
		})

		cluster.Reserve(reservationId, uint64(chunk.Size))
		chunks = chunks[1:]
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
