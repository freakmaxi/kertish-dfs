package manager

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/freakmaxi/kertish-dfs/basics/common"
)

const managerEndPoint = "/client/node"
const bulkRequestInterval = 100    // milliseconds
const bulkRequestRetryInterval = 5 // seconds
const notificationChannelLimit = 100
const notificationBulkLimit = 20

type Node interface {
	Join(clusterId string, nodeId string, masterAddress string)
	Mode(master bool)
	Leave()
	Handshake(hardwareAddr string, bindAddr string, size uint64) error

	Create(sha512Hex string)
	Delete(sha512Hex string, shadow bool, size uint32)

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

	createNotificationChan chan string
	deleteNotificationChan chan common.SyncDelete
}

func NewNode(managerAddresses []string) Node {
	node := &node{
		client:                 http.Client{},
		managerAddr:            managerAddresses,
		createNotificationChan: make(chan string, notificationChannelLimit),
		deleteNotificationChan: make(chan common.SyncDelete, notificationChannelLimit),
	}
	node.start()

	return node
}

func (n *node) start() {
	go n.createChannelHandler()
	go n.deleteChannelHandler()
}

func (n *node) createChannelHandler() {
	sha512HexList := make([]string, 0)
	for {
		select {
		case sha512Hex, more := <-n.createNotificationChan:
			if !more {
				return
			}

			sha512HexList = append(sha512HexList, sha512Hex)
			if len(sha512HexList) >= notificationBulkLimit {
				go n.createBulk(sha512HexList)
				sha512HexList = make([]string, 0)
			}
		default:
			if len(sha512HexList) > 0 {
				go n.createBulk(sha512HexList)
				sha512HexList = make([]string, 0)

				continue
			}
			<-time.After(time.Millisecond * bulkRequestInterval)
		}
	}
}

func (n *node) createBulk(sha512HexList []string) {
	for {
		if err := n.create(sha512HexList); err == nil {
			fmt.Printf("WARN: Bulk (CREATE) notification is fail: %s", err)
			<-time.After(time.Second * bulkRequestRetryInterval)
			continue
		}
		return
	}
}

func (n *node) create(sha512HexList []string) error {
	body, err := json.Marshal(sha512HexList)
	if err != nil {
		return err
	}

	req, err := http.NewRequest("POST", fmt.Sprintf("%s%s", n.managerAddr[0], managerEndPoint), bytes.NewBuffer(body))
	if err != nil {
		return err
	}
	req.Header.Set("X-Action", "create")
	req.Header.Set("X-Options", n.nodeId)

	res, err := n.client.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()

	if res.StatusCode != 202 {
		if res.StatusCode == 404 {
			return fmt.Errorf("data node is not registered")
		}
		return fmt.Errorf("node manager request is failed (Create): %d - %s", res.StatusCode, common.NewErrorFromReader(res.Body).Message)
	}

	return nil
}

func (n *node) deleteChannelHandler() {
	syncDeleteList := make(common.SyncDeleteList, 0)
	for {
		select {
		case syncDelete, more := <-n.deleteNotificationChan:
			if !more {
				return
			}

			syncDeleteList = append(syncDeleteList, syncDelete)
			if len(syncDeleteList) >= notificationBulkLimit {
				go n.deleteBulk(syncDeleteList)
				syncDeleteList = make(common.SyncDeleteList, 0)
			}
		default:
			if len(syncDeleteList) > 0 {
				go n.deleteBulk(syncDeleteList)
				syncDeleteList = make(common.SyncDeleteList, 0)

				continue
			}
			<-time.After(time.Millisecond * bulkRequestInterval)
		}
	}
}

func (n *node) deleteBulk(syncDeleteList common.SyncDeleteList) {
	for {
		if err := n.delete(syncDeleteList); err == nil {
			fmt.Printf("WARN: Bulk (DELETE) notification is fail: %s", err)
			<-time.After(time.Second * bulkRequestRetryInterval)
			continue
		}
		return
	}
}

func (n *node) delete(syncDeleteList common.SyncDeleteList) error {
	body, err := json.Marshal(syncDeleteList)
	if err != nil {
		return err
	}

	req, err := http.NewRequest("DELETE", fmt.Sprintf("%s%s", n.managerAddr[0], managerEndPoint), bytes.NewBuffer(body))
	if err != nil {
		return err
	}
	req.Header.Set("X-Action", "delete")
	req.Header.Set("X-Options", n.nodeId)

	res, err := n.client.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()

	if res.StatusCode != 200 {
		if res.StatusCode == 404 {
			return fmt.Errorf("data node is not registered")
		}
		return fmt.Errorf("node manager request is failed (Delete): %d - %s", res.StatusCode, common.NewErrorFromReader(res.Body).Message)
	}

	return nil
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

func (n *node) Handshake(hardwareAddr string, bindAddr string, size uint64) error {
	req, err := http.NewRequest("POST", fmt.Sprintf("%s%s", n.managerAddr[0], managerEndPoint), nil)
	if err != nil {
		return err
	}
	req.Header.Set("X-Action", "handshake")
	req.Header.Set("X-Options", fmt.Sprintf("%s,%s,%s", strconv.FormatUint(size, 10), hardwareAddr, bindAddr))

	res, err := n.client.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()

	if res.StatusCode != 200 {
		if res.StatusCode == 404 {
			return fmt.Errorf("data node is not registered")
		}
		return fmt.Errorf("node manager request is failed (Handshake): %d - %s", res.StatusCode, common.NewErrorFromReader(res.Body).Message)
	}

	n.clusterId = res.Header.Get("X-Cluster-Id")
	n.nodeId = res.Header.Get("X-Node-Id")
	if len(n.clusterId) == 0 || len(n.nodeId) == 0 {
		return fmt.Errorf("node manager response wrong for handshake")
	}
	n.masterAddress = res.Header.Get("X-Master")

	return nil
}

func (n *node) Create(sha512Hex string) {
	n.createNotificationChan <- sha512Hex
}

func (n *node) Delete(sha512Hex string, shadow bool, size uint32) {
	n.deleteNotificationChan <- common.SyncDelete{
		Sha512Hex: sha512Hex,
		Shadow:    shadow,
		Size:      size,
	}
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
