package manager

import (
	"encoding/json"
	errors2 "errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"

	"github.com/freakmaxi/kertish-dfs/basics/common"
	"github.com/freakmaxi/kertish-dfs/basics/errors"
	cluster2 "github.com/freakmaxi/kertish-dfs/head-node/cluster"
	"go.uber.org/zap"
)

const managerEndPoint = "/client/manager"

type Cluster interface {
	Create(size uint64, reader io.Reader) (*common.CreationResult, error)
	CreateShadow(chunks common.DataChunks) error
	Read(chunks common.DataChunks) (func(w io.Writer, begins int64, ends int64) error, error)
	Delete(chunks common.DataChunks) (*common.DeletionResult, error)
}

type cluster struct {
	client      http.Client
	managerAddr []string
	logger      *zap.Logger

	nodeCacheMutex sync.Mutex
	nodeCache      map[string]cluster2.DataNode
}

func NewCluster(managerAddresses []string, logger *zap.Logger) (Cluster, error) {
	if len(managerAddresses) == 0 {
		return nil, os.ErrInvalid
	}

	return &cluster{
		client:         http.Client{},
		managerAddr:    managerAddresses,
		logger:         logger,
		nodeCacheMutex: sync.Mutex{},
		nodeCache:      make(map[string]cluster2.DataNode),
	}, nil
}

func (c *cluster) getDataNode(address string) (cluster2.DataNode, error) {
	c.nodeCacheMutex.Lock()
	defer c.nodeCacheMutex.Unlock()

	dn, has := c.nodeCache[address]
	if !has {
		var err error
		dn, err = cluster2.NewDataNode(address)
		if err != nil {
			return nil, err
		}
		c.nodeCache[address] = dn
	}

	return dn, nil
}

func (c *cluster) Create(size uint64, reader io.Reader) (*common.CreationResult, error) {
	reservation, err := c.makeReservation(size)
	if err != nil {
		return nil, err
	}

	create := NewCreate(reservation, c.getDataNode, c.findCluster, c.logger)
	creationResult, clusterUsageMap, err := create.process(reader)
	if err != nil {
		if err := c.discardReservation(reservation.Id); err != nil {
			c.logger.Error(
				"Discarding reservationMap is failed",
				zap.String("reservationId", reservation.Id),
				zap.Error(err),
			)
		}
		return nil, err
	}

	if err := c.commitReservation(reservation.Id, clusterUsageMap); err != nil {
		c.logger.Error(
			"Committing reservationMap is failed",
			zap.String("reservationId", reservation.Id),
			zap.Error(err),
		)
	}

	return creationResult, nil
}

func (c *cluster) CreateShadow(chunks common.DataChunks) error {
	m, err := c.createClusterMap(chunks, common.MTCreate)
	if err != nil {
		if err == errors.ErrNotFound {
			return errors.ErrZombie
		}
		return err
	}

	for _, chunk := range chunks {
		if address, has := m[chunk.Hash]; has {
			dn, err := c.getDataNode(address[0])
			if err != nil {
				return err
			}

			if err := dn.CreateShadow(chunk.Hash); err != nil {
				return err
			}
		}
	}

	return nil
}

func (c *cluster) Read(chunks common.DataChunks) (func(w io.Writer, begins int64, ends int64) error, error) {
	sort.Sort(chunks)

	m, err := c.createClusterMap(chunks, common.MTRead)
	if err != nil {
		if err == errors.ErrNotFound {
			return nil, errors.ErrZombie
		}
		return nil, err
	}

	return func(w io.Writer, begins int64, ends int64) error {
		chunkTotal := int64(0)
		for _, chunk := range chunks {
			chunkSize := int64(chunk.Size)
			chunkTotal += chunkSize

			if chunkTotal < begins {
				continue
			}
			if ends > -1 && ends < (chunkTotal-chunkSize) {
				break
			}

			trimmingSize := chunkTotal - begins
			alignmentPoint := chunkTotal - trimmingSize
			startPoint := alignmentPoint - (chunkTotal - chunkSize)
			if startPoint < 0 {
				startPoint = 0
			}

			endPoint := chunkSize
			if ends > -1 {
				endsCal := (chunkTotal - 1) - ends
				if endsCal > 0 && endsCal < chunkSize {
					endPoint -= endsCal
				}
			}

			addresses, has := m[chunk.Hash]
			if !has {
				return errors.ErrRepair
			}

			bulkErrors := errors.NewBulkError()
			for _, address := range addresses {
				dn, err := c.getDataNode(address)
				if err != nil {
					bulkErrors.Add(err)
					continue
				}

				if err := dn.Read(chunk.Hash, uint32(startPoint), uint32(endPoint), func(buffer []byte) error {
					if int64(len(buffer)) != endPoint-startPoint {
						return errors.ErrRepair
					}
					_, err := w.Write(buffer)
					if errors2.Is(err, syscall.EPIPE) {
						return nil
					}
					return err
				}); err != nil {
					if errors.IsDialError(err) {
						bulkErrors.Add(err)
						continue
					}
					return err
				}

				bulkErrors = nil
				break
			}

			if bulkErrors != nil {
				return bulkErrors
			}
		}

		return nil
	}, nil
}

func (c *cluster) Delete(chunks common.DataChunks) (*common.DeletionResult, error) {
	if len(chunks) == 0 {
		return nil, errors.ErrZombie
	}

	m, err := c.createClusterMap(chunks, common.MTDelete)
	if err != nil {
		if err == errors.ErrNotFound {
			return nil, errors.ErrZombie
		}
		return nil, err
	}

	deletionResult := common.NewDeletionResult()

	for _, chunk := range chunks {
		address, has := m[chunk.Hash]
		if !has {
			deletionResult.Missing = append(deletionResult.Missing, chunk.Hash)
			continue
		}

		dn, err := c.getDataNode(address[0])
		if err != nil {
			deletionResult.Untouched = append(deletionResult.Untouched, chunk.Hash)
			continue
		}

		if err := dn.Delete(chunk.Hash); err != nil {
			deletionResult.Untouched = append(deletionResult.Untouched, chunk.Hash)
			continue
		}

		deletionResult.Deleted = append(deletionResult.Deleted, chunk.Hash)
	}

	return &deletionResult, nil
}

func (c *cluster) makeReservation(size uint64) (*common.ReservationMap, error) {
	req, err := http.NewRequest("POST", fmt.Sprintf("%s%s", c.managerAddr[0], managerEndPoint), nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("X-Action", "reserve")
	req.Header.Set("X-Size", strconv.FormatUint(size, 10))

	res, err := c.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = res.Body.Close() }()

	if res.StatusCode != 200 {
		if res.StatusCode == 503 {
			return nil, errors.ErrNoAvailableClusterNode
		}
		if res.StatusCode == 507 {
			return nil, errors.ErrNoSpace
		}
		return nil, fmt.Errorf("cluster manager request is failed (makeReservation): %d - %s", res.StatusCode, common.NewErrorFromReader(res.Body).Message)
	}

	var reservationMap common.ReservationMap
	if err := json.NewDecoder(res.Body).Decode(&reservationMap); err != nil {
		return nil, err
	}

	return &reservationMap, nil
}

func (c *cluster) discardReservation(reservationId string) error {
	req, err := http.NewRequest("DELETE", fmt.Sprintf("%s%s", c.managerAddr[0], managerEndPoint), nil)
	if err != nil {
		return err
	}
	req.Header.Set("X-Action", "discard")
	req.Header.Set("X-Reservation-Id", reservationId)

	res, err := c.client.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = res.Body.Close() }()

	if res.StatusCode != 200 {
		return fmt.Errorf("cluster manager request is failed (discardReservation): %d - %s", res.StatusCode, common.NewErrorFromReader(res.Body).Message)
	}

	return nil
}

func (c *cluster) commitReservation(reservationId string, clusterUsageMap map[string]uint64) error {
	req, err := http.NewRequest("DELETE", fmt.Sprintf("%s%s", c.managerAddr[0], managerEndPoint), nil)
	if err != nil {
		return err
	}
	req.Header.Set("X-Action", "commit")
	req.Header.Set("X-Reservation-Id", reservationId)

	clusterUsageList := make([]string, 0)
	for k, v := range clusterUsageMap {
		clusterUsageList = append(clusterUsageList, fmt.Sprintf("%s=%d", k, v))
	}
	req.Header.Set("X-Options", strings.Join(clusterUsageList, ","))

	res, err := c.client.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = res.Body.Close() }()

	if res.StatusCode != 200 {
		return fmt.Errorf("cluster manager request is failed (commitReservation): %d - %s", res.StatusCode, common.NewErrorFromReader(res.Body).Message)
	}

	return nil
}

func (c *cluster) findCluster(sha512Hex string) (string, string, error) {
	req, err := http.NewRequest("GET", fmt.Sprintf("%s%s", c.managerAddr[0], managerEndPoint), nil)
	if err != nil {
		return "", "", err
	}
	req.Header.Set("X-Action", "find")
	req.Header.Set("X-Options", sha512Hex)

	res, err := c.client.Do(req)
	if err != nil {
		c.logger.Error(
			"cluster manager request is failed (findCluster)",
			zap.Error(err),
		)
		return "", "", errors.ErrRemote
	}
	defer func() { _ = res.Body.Close() }()

	if res.StatusCode == 200 {
		return res.Header.Get("X-Cluster-Id"), res.Header.Get("X-Address"), nil
	}

	if res.StatusCode == 404 {
		return "", "", errors.ErrNotFound
	} else if res.StatusCode == 503 {
		return "", "", errors.ErrNoAvailableClusterNode
	}

	c.logger.Error(
		fmt.Sprintf(
			"cluster manager request is failed (findCluster): %d - %s",
			res.StatusCode,
			common.NewErrorFromReader(res.Body).Message,
		),
	)

	return "", "", errors.ErrRemote
}

func (c *cluster) createClusterMap(chunks common.DataChunks, mapType common.MapType) (map[string][]string, error) {
	sha512HexList := make([]string, 0)
	for _, chunk := range chunks {
		sha512HexList = append(sha512HexList, chunk.Hash)
	}

	m, err := c.requestClusterMap(sha512HexList, mapType)
	if err != nil {
		return nil, err
	}

	return m, nil
}

func (c *cluster) requestClusterMap(sha512HexList []string, mapType common.MapType) (map[string][]string, error) {
	req, err := http.NewRequest("POST", fmt.Sprintf("%s%s", c.managerAddr[0], managerEndPoint), nil)
	if err != nil {
		return nil, err
	}

	mode := "read"
	switch mapType {
	case common.MTCreate:
		mode = "create"
	case common.MTDelete:
		mode = "delete"
	}
	req.Header.Set("X-Action", fmt.Sprintf("%sMap", mode))
	req.Header.Set("X-Options", strings.Join(sha512HexList, ","))

	res, err := c.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = res.Body.Close() }()

	if res.StatusCode != 200 {
		if res.StatusCode == 404 {
			return nil, errors.ErrNotFound
		}
		if res.StatusCode == 503 {
			return nil, errors.ErrNoAvailableActionNode
		}

		return nil, fmt.Errorf("cluster manager request is failed (requestClusterMap): %d - %s", res.StatusCode, common.NewErrorFromReader(res.Body).Message)
	}

	var clusterMapping map[string][]string
	if err := json.NewDecoder(res.Body).Decode(&clusterMapping); err != nil {
		return nil, err
	}

	return clusterMapping, nil
}

var _ Cluster = &cluster{}
