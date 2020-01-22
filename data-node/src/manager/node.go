package manager

import (
	"fmt"
	"net/http"
	"strconv"

	"github.com/freakmaxi/kertish-dfs/data-node/src/common"
)

const managerEndPoint = "/client/node"

type Node interface {
	Join(clusterId string, nodeId string, masterAddress string)
	Mode(master bool)
	Leave()
	Handshake(bindAddr string, size uint64) error

	Create(sha512Hex string) error
	Delete(sha512Hex string, shadow bool, size uint32) error

	ClusterId() string
	NodeId() string
	MasterAddress() string
}

type node struct {
	client      http.Client
	managerAddr []string

	clusterId     string
	nodeId        string
	masterAddress string
}

func NewNode(managerAddresses []string) (Node, error) {
	return &node{
		client:      http.Client{},
		managerAddr: managerAddresses,
	}, nil
}

func (n *node) Join(clusterId string, nodeId string, masterAddress string) {
	if len(n.clusterId) > 0 && len(n.nodeId) > 0 {
		return
	}

	n.clusterId = clusterId
	n.nodeId = nodeId
	n.masterAddress = masterAddress

	mode := "SLAVE"
	if len(n.masterAddress) == 0 {
		mode = "MASTER"
	}

	fmt.Printf("INFO: Data Node is joined to cluster (%s) with node id (%s) as %s\n", clusterId, nodeId, mode)
}

func (n *node) Mode(master bool) {
	if master && len(n.masterAddress) == 0 {
		return
	}

	mode := "SLAVE"
	if master {
		n.masterAddress = ""
		mode = "MASTER"
	}
	fmt.Printf("INFO: Data Node (%s) is marked as %s\n", n.nodeId, mode)
}

func (n *node) Leave() {
	fmt.Printf("INFO: Data Node is deleted from cluster (%s). Now working as stand-alone\n", n.clusterId)

	n.clusterId = ""
	n.nodeId = ""
	n.masterAddress = ""
}

func (n *node) Handshake(bindAddr string, size uint64) error {
	req, err := http.NewRequest("POST", fmt.Sprintf("%s%s", n.managerAddr[0], managerEndPoint), nil)
	if err != nil {
		return err
	}
	req.Header.Set("X-Action", "handshake")
	req.Header.Set("X-Options", fmt.Sprintf("%s,%s", strconv.FormatUint(size, 10), bindAddr))

	res, err := n.client.Do(req)
	if err != nil {
		return err
	}

	if res.StatusCode != 200 {
		if res.StatusCode == 404 {
			return fmt.Errorf("data node is not registered")
		}
		return fmt.Errorf("node manager request is failed (Handshake): %d - %s", res.StatusCode, common.NewError(res.Body).Message)
	}

	n.clusterId = res.Header.Get("X-ClusterId")
	n.nodeId = res.Header.Get("X-NodeId")
	if len(n.clusterId) == 0 || len(n.nodeId) == 0 {
		return fmt.Errorf("node manager response wrong for handshake")
	}
	n.masterAddress = res.Header.Get("X-Master")

	return nil
}

func (n *node) Create(sha512Hex string) error {
	req, err := http.NewRequest("POST", fmt.Sprintf("%s%s", n.managerAddr[0], managerEndPoint), nil)
	if err != nil {
		return err
	}
	req.Header.Set("X-Action", "create")
	req.Header.Set("X-Options", fmt.Sprintf("%s,%s", n.nodeId, sha512Hex))

	res, err := n.client.Do(req)
	if err != nil {
		return err
	}

	if res.StatusCode != 202 {
		if res.StatusCode == 404 {
			return fmt.Errorf("data node is not registered")
		}
		return fmt.Errorf("node manager request is failed (Create): %d - %s", res.StatusCode, common.NewError(res.Body).Message)
	}

	return nil
}

func (n *node) Delete(sha512Hex string, shadow bool, size uint32) error {
	req, err := http.NewRequest("DELETE", fmt.Sprintf("%s%s", n.managerAddr[0], managerEndPoint), nil)
	if err != nil {
		return err
	}
	shadowString := "0"
	if shadow {
		shadowString = "1"
	}
	req.Header.Set("X-Action", "delete")
	req.Header.Set("X-Options", fmt.Sprintf("%s,%s,%s,%d", n.nodeId, sha512Hex, shadowString, size))

	res, err := n.client.Do(req)
	if err != nil {
		return err
	}

	if res.StatusCode != 200 {
		if res.StatusCode == 404 {
			return fmt.Errorf("data node is not registered")
		}
		return fmt.Errorf("node manager request is failed (Delete): %d - %s", res.StatusCode, common.NewError(res.Body).Message)
	}

	return nil
}

func (n *node) ClusterId() string {
	return n.clusterId
}

func (n *node) NodeId() string {
	return n.nodeId
}

func (n *node) MasterAddress() string {
	return n.masterAddress
}
