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
	"syscall"

	"github.com/freakmaxi/kertish-dfs/basics/common"
	"github.com/freakmaxi/kertish-dfs/basics/errors"
	cluster2 "github.com/freakmaxi/kertish-dfs/head-node/src/cluster"
)

const managerEndPoint = "/client/manager"

type Cluster interface {
	Create(size uint64, reader io.Reader) (common.DataChunks, error)
	CreateShadow(chunks common.DataChunks) error
	Read(chunks common.DataChunks, writer io.Writer, begins int64, ends int64) error
	Delete(chunks common.DataChunks) ([]string, []string, error)
}

type cluster struct {
	client      http.Client
	managerAddr []string
}

func NewCluster(managerAddresses []string) (Cluster, error) {
	if len(managerAddresses) == 0 {
		return nil, os.ErrInvalid
	}

	return &cluster{
		client:      http.Client{},
		managerAddr: managerAddresses,
	}, nil
}

func (c *cluster) Create(size uint64, reader io.Reader) (common.DataChunks, error) {
	reservation, err := c.makeReservation(size)
	if err != nil {
		return nil, err
	}

	create := NewCreate(reservation)
	chunks, clusterUsageMap, err := create.process(reader, c.findCluster)
	if err != nil {
		if err := c.discardReservation(reservation.Id); err != nil {
			fmt.Printf("ERROR: Discarding reservationMap (%s) is failed: %s\n", reservation.Id, err.Error())
		}
		return nil, err
	}

	if err := c.commitReservation(reservation.Id, clusterUsageMap); err != nil {
		fmt.Printf("ERROR: Committing reservationMap (%s) is failed: %s\n", reservation.Id, err.Error())
	}

	return chunks, nil
}

func (c *cluster) CreateShadow(chunks common.DataChunks) error {
	m, err := c.createClusterMap(chunks, common.MT_Create)
	if err != nil {
		return err
	}

	for _, chunk := range chunks {
		if address, has := m[chunk.Hash]; has {
			if err := cluster2.NewDataNode(address).CreateShadow(chunk.Hash); err != nil {
				return err
			}
		}
	}

	return nil
}

func (c *cluster) Read(chunks common.DataChunks, w io.Writer, begins int64, ends int64) error {
	sort.Sort(chunks)

	m, err := c.createClusterMap(chunks, common.MT_Read)
	if err != nil {
		return err
	}

	total := int64(0)
	for _, chunk := range chunks {
		chunkSize := int64(chunk.Size)

		total += chunkSize
		if total < begins {
			continue
		}
		if ends > -1 && ends < total-chunkSize {
			break
		}

		startPoint := total - chunkSize
		startPoint = begins - startPoint
		if startPoint < 0 {
			startPoint = 0
		}

		endPoint := chunkSize
		if ends > -1 {
			endsCal := (total - 1) - ends
			if endsCal > 0 && endsCal < chunkSize {
				endPoint -= endsCal
			}
		}

		address, has := m[chunk.Hash]
		if !has {
			continue
		}

		if err := cluster2.NewDataNode(address).Read(chunk.Hash, func(buffer []byte) error {
			_, err := w.Write(buffer[startPoint:endPoint])
			if errors2.Is(err, syscall.EPIPE) {
				return nil
			}
			return err
		}); err != nil {
			return err
		}
	}

	return nil
}

func (c *cluster) Delete(chunks common.DataChunks) ([]string, []string, error) {
	deletedHashes := make([]string, 0)
	missingHashes := make([]string, 0)

	m, err := c.createClusterMap(chunks, common.MT_Delete)
	if err != nil {
		return deletedHashes, missingHashes, err
	}

	for _, chunk := range chunks {
		address, has := m[chunk.Hash]
		if !has {
			missingHashes = append(missingHashes, chunk.Hash)
			continue
		}
		if err := cluster2.NewDataNode(address).Delete(chunk.Hash); err != nil {
			return deletedHashes, missingHashes, err
		}
		deletedHashes = append(deletedHashes, chunk.Hash)
	}

	return deletedHashes, missingHashes, nil
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

	if res.StatusCode != 200 {
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
		return "", "", err
	}

	if res.StatusCode != 200 {
		if res.StatusCode == 404 {
			return "", "", errors.ErrNoAvailableActionNode
		}
		return "", "", fmt.Errorf("cluster manager request is failed (findCluster): %d - %s", res.StatusCode, common.NewErrorFromReader(res.Body).Message)
	}

	return res.Header.Get("X-Cluster-Id"), res.Header.Get("X-Address"), nil
}

func (c *cluster) createClusterMap(chunks common.DataChunks, mapType common.MapType) (map[string]string, error) {
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

func (c *cluster) requestClusterMap(sha512HexList []string, mapType common.MapType) (map[string]string, error) {
	req, err := http.NewRequest("POST", fmt.Sprintf("%s%s", c.managerAddr[0], managerEndPoint), nil)
	if err != nil {
		return nil, err
	}

	mode := "read"
	switch mapType {
	case common.MT_Create:
		mode = "create"
	case common.MT_Delete:
		mode = "delete"
	}
	req.Header.Set("X-Action", fmt.Sprintf("%sMap", mode))
	req.Header.Set("X-Options", strings.Join(sha512HexList, ","))

	res, err := c.client.Do(req)
	if err != nil {
		return nil, err
	}

	if res.StatusCode != 200 {
		if res.StatusCode == 503 {
			return nil, errors.ErrNoAvailableActionNode
		}

		return nil, fmt.Errorf("cluster manager request is failed (requestClusterMap): %d - %s", res.StatusCode, common.NewErrorFromReader(res.Body).Message)
	}

	var clusterMapping map[string]string
	if err := json.NewDecoder(res.Body).Decode(&clusterMapping); err != nil {
		return nil, err
	}

	return clusterMapping, nil
}

var _ Cluster = &cluster{}
