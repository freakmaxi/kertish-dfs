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

	cluster2 "github.com/freakmaxi/2020-dfs/head-node/src/cluster"
	"github.com/freakmaxi/2020-dfs/head-node/src/common"
	"github.com/freakmaxi/2020-dfs/head-node/src/errors"
)

const managerEndPoint = "/client/manager"

type Cluster interface {
	Create(size uint64, reader io.Reader) (common.DataChunks, error)
	CreateShadow(chunks common.DataChunks) error
	Read(chunks common.DataChunks, writer io.Writer, begins int64, ends int64) error
	Delete(chunks common.DataChunks) error
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
			fmt.Printf("ERROR: Discarding reservation (%s) is failed: %s\n", reservation.Id, err.Error())
		}
		return nil, err
	}

	if err := c.commitReservation(reservation.Id, clusterUsageMap); err != nil {
		fmt.Printf("ERROR: Commiting reservation (%s) is failed: %s\n", reservation.Id, err.Error())
	}

	return chunks, nil
}

func (c *cluster) CreateShadow(chunks common.DataChunks) error {
	m, err := c.createClusterMap(chunks, true)
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

	m, err := c.createClusterMap(chunks, false)
	if err != nil {
		return err
	}

	total := int64(0)
	for _, chunk := range chunks {
		total += int64(chunk.Size)
		if total < begins {
			continue
		}
		if ends > -1 && ends < total-int64(chunk.Size) {
			break
		}

		if address, has := m[chunk.Hash]; has {
			chunkStarts := total - int64(chunk.Size)
			begins -= chunkStarts
			if begins < 0 {
				begins = 0
			}

			endPoint := int64(chunk.Size)
			if ends > -1 {
				endsCal := (total - 1) - ends
				if endsCal > 0 && endsCal < endPoint {
					endPoint -= endsCal
				}
			}

			if err := cluster2.NewDataNode(address).Read(chunk.Hash, func(buffer []byte) error {
				_, err := w.Write(buffer[begins:endPoint])
				if errors2.Is(err, syscall.EPIPE) {
					return nil
				}
				return err
			}); err != nil {
				return err
			}
		}
	}

	return nil
}

func (c *cluster) Delete(chunks common.DataChunks) error {
	m, err := c.createClusterMap(chunks, true)
	if err != nil {
		return err
	}

	for _, chunk := range chunks {
		if address, has := m[chunk.Hash]; has {
			if err := cluster2.NewDataNode(address).Delete(chunk.Hash); err != nil {
				return err
			}
		}
	}

	return nil
}

func (c *cluster) makeReservation(size uint64) (*common.Reservation, error) {
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
		return nil, fmt.Errorf("cluster manager request is failed (makeReservation): %d - %s", res.StatusCode, common.NewError(res.Body).Message)
	}

	var reservation common.Reservation
	if err := json.NewDecoder(res.Body).Decode(&reservation); err != nil {
		return nil, err
	}

	return &reservation, nil
}

func (c *cluster) discardReservation(reservationId string) error {
	req, err := http.NewRequest("DELETE", fmt.Sprintf("%s%s", c.managerAddr[0], managerEndPoint), nil)
	if err != nil {
		return err
	}
	req.Header.Set("X-Action", "discard")
	req.Header.Set("X-ReservationId", reservationId)

	res, err := c.client.Do(req)
	if err != nil {
		return err
	}

	if res.StatusCode != 200 {
		return fmt.Errorf("cluster manager request is failed (discardReservation): %d - %s", res.StatusCode, common.NewError(res.Body).Message)
	}

	return nil
}

func (c *cluster) commitReservation(reservationId string, clusterUsageMap map[string]uint64) error {
	req, err := http.NewRequest("DELETE", fmt.Sprintf("%s%s", c.managerAddr[0], managerEndPoint), nil)
	if err != nil {
		return err
	}
	req.Header.Set("X-Action", "commit")
	req.Header.Set("X-ReservationId", reservationId)

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
		return fmt.Errorf("cluster manager request is failed (commitReservation): %d - %s", res.StatusCode, common.NewError(res.Body).Message)
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
			return "", "", errors.ErrNoAvailableNode
		}
		return "", "", fmt.Errorf("cluster manager request is failed (findCluster): %d - %s", res.StatusCode, common.NewError(res.Body).Message)
	}

	return res.Header.Get("X-ClusterId"), res.Header.Get("X-Address"), nil
}

func (c *cluster) createClusterMap(chunks common.DataChunks, deleteMap bool) (map[string]string, error) {
	sha512HexList := make([]string, 0)
	for _, chunk := range chunks {
		sha512HexList = append(sha512HexList, chunk.Hash)
	}

	m, err := c.requestClusterMap(sha512HexList, deleteMap)
	if err != nil {
		return nil, err
	}

	return m, nil
}

func (c *cluster) requestClusterMap(sha512HexList []string, deleteMap bool) (map[string]string, error) {
	req, err := http.NewRequest("POST", fmt.Sprintf("%s%s", c.managerAddr[0], managerEndPoint), nil)
	if err != nil {
		return nil, err
	}
	mode := "read"
	if deleteMap {
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
			return nil, errors.ErrNoAvailableNode
		}

		return nil, fmt.Errorf("cluster manager request is failed (requestClusterMap): %d - %s", res.StatusCode, common.NewError(res.Body).Message)
	}

	var clusterMapping map[string]string
	if err := json.NewDecoder(res.Body).Decode(&clusterMapping); err != nil {
		return nil, err
	}

	return clusterMapping, nil
}

var _ Cluster = &cluster{}
