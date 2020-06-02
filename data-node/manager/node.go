package manager

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/freakmaxi/kertish-dfs/basics/common"
	"go.uber.org/zap"
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

	Create(sha512Hex string, size uint32)
	Delete(sha512Hex string, shadow bool, size uint32)

	ClusterId() string
	NodeId() string
	MasterAddress() string

	NodeSize() uint64
}

type node struct {
	client      http.Client
	managerAddr []string
	nodeSize    uint64
	logger      *zap.Logger

	clusterId     string
	nodeId        string
	masterAddress string

	createNotificationChan chan common.SyncFileItem
	createFailureChan      chan common.SyncFileItemList
	deleteNotificationChan chan common.SyncDelete
	deleteFailureChan      chan common.SyncDeleteList
}

func NewNode(managerAddresses []string, nodeSize uint64, logger *zap.Logger) Node {
	node := &node{
		client:                 http.Client{},
		managerAddr:            managerAddresses,
		nodeSize:               nodeSize,
		logger:                 logger,
		createNotificationChan: make(chan common.SyncFileItem, notificationChannelLimit),
		createFailureChan:      make(chan common.SyncFileItemList, notificationChannelLimit),
		deleteNotificationChan: make(chan common.SyncDelete, notificationChannelLimit),
		deleteFailureChan:      make(chan common.SyncDeleteList, notificationChannelLimit),
	}
	node.start()

	return node
}

func (n *node) start() {
	go n.createChannelHandler()
	go n.deleteChannelHandler()
}

func (n *node) createChannelHandler() {
	fileItemList := make(common.SyncFileItemList, 0)
	for {
		select {
		case fileItem, more := <-n.createNotificationChan:
			if !more {
				return
			}

			fileItemList = append(fileItemList, fileItem)
			if len(fileItemList) >= notificationBulkLimit {
				go n.createBulk(fileItemList, 0)
				fileItemList = make(common.SyncFileItemList, 0)
			}
		case failedFileItemList, more := <-n.createFailureChan:
			if !more {
				return
			}
			go n.createBulk(failedFileItemList, time.Second*bulkRequestRetryInterval)
		default:
			if len(fileItemList) == 0 {
				<-time.After(time.Millisecond * bulkRequestInterval)
				continue
			}

			go n.createBulk(fileItemList, 0)
			fileItemList = make(common.SyncFileItemList, 0)
		}
	}
}

func (n *node) createBulk(fileItemList common.SyncFileItemList, delay time.Duration) {
	if delay > 0 {
		<-time.After(delay)
	}

	if err := n.create(fileItemList); err != nil {
		n.logger.Warn("Bulk (CREATE) notification is failed", zap.Error(err))
		n.createFailureChan <- fileItemList
	}
}

func (n *node) create(fileItemList common.SyncFileItemList) error {
	body, err := json.Marshal(fileItemList)
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
				go n.deleteBulk(syncDeleteList, 0)
				syncDeleteList = make(common.SyncDeleteList, 0)
			}
		case failedSyncDeleteList, more := <-n.deleteFailureChan:
			if !more {
				return
			}
			go n.deleteBulk(failedSyncDeleteList, time.Second*bulkRequestRetryInterval)
		default:
			if len(syncDeleteList) == 0 {
				<-time.After(time.Millisecond * bulkRequestInterval)
				continue
			}

			go n.deleteBulk(syncDeleteList, 0)
			syncDeleteList = make(common.SyncDeleteList, 0)
		}
	}
}

func (n *node) deleteBulk(syncDeleteList common.SyncDeleteList, delay time.Duration) {
	if delay > 0 {
		<-time.After(delay)
	}

	if err := n.delete(syncDeleteList); err != nil {
		n.logger.Warn("Bulk (DELETE) notification is failed", zap.Error(err))
		n.deleteFailureChan <- syncDeleteList
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

	n.logger.Sugar().Infof("Data Node is joined to cluster (%s) with node id (%s) as %s", clusterId, nodeId, mode)
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
	n.logger.Sugar().Infof("Data Node (%s) is marked as %s", n.nodeId, mode)
}

func (n *node) Leave() {
	n.logger.Sugar().Infof("Data Node is deleted from cluster (%s). Now working as stand-alone", n.clusterId)

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

func (n *node) Create(sha512Hex string, size uint32) {
	n.createNotificationChan <- common.SyncFileItem{
		Sha512Hex: sha512Hex,
		Size:      int32(size),
	}
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

func (n *node) NodeSize() uint64 {
	return n.nodeSize
}

var _ Node = &node{}
