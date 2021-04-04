package manager

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"sync"
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
	Handshake() error

	Notify(sha512Hex string, usage uint16, size uint32, shadow bool, create bool) <-chan bool

	ClusterId() string
	NodeId() string
	MasterAddress() string

	HardwareAddr() string
	BindAddr() string
	NodeSize() uint64
}

type node struct {
	hardwareAddr string
	bindAddr     string
	nodeSize     uint64

	client      http.Client
	managerAddr []string
	logger      *zap.Logger

	clusterId     string
	nodeId        string
	masterAddress string

	notificationChan chan common.NotificationContainer
	failureChan      chan common.NotificationContainerList

	nextProcessList map[string]*common.NotificationContainer
}

func NewNode(hardwareAddr string, bindAddr string, nodeSize uint64, managerAddresses []string, logger *zap.Logger) Node {
	node := &node{
		hardwareAddr: hardwareAddr,
		bindAddr:     bindAddr,
		nodeSize:     nodeSize,

		client:      http.Client{},
		managerAddr: managerAddresses,
		logger:      logger,

		notificationChan: make(chan common.NotificationContainer, notificationChannelLimit),
		failureChan:      make(chan common.NotificationContainerList, notificationChannelLimit),

		nextProcessList: make(map[string]*common.NotificationContainer),
	}
	node.start()

	return node
}

func (n *node) start() {
	go n.channelHandler()
}

func (n *node) channelHandler() {
	for {
		select {
		case failedList, more := <-n.failureChan:
			if !more {
				return
			}

			if len(n.nextProcessList) == 0 {
				time.Sleep(time.Second * bulkRequestRetryInterval)
			}

			for _, nc := range failedList {
				if _, has := n.nextProcessList[nc.FileItem.Sha512Hex]; has {
					nc.ResponseChan <- true
					continue
				}
				n.nextProcessList[nc.FileItem.Sha512Hex] = nc
			}
			continue
		case nc, more := <-n.notificationChan:
			if !more {
				return
			}

			n.nextProcessList[nc.FileItem.Sha512Hex] = &nc
			continue
		default:
			if len(n.nextProcessList) == 0 {
				time.Sleep(time.Millisecond * bulkRequestInterval)
				continue
			}
		}

		n.notifyBulk()
	}
}

func (n *node) createBulkGroup() []common.NotificationContainerList {
	bulkGroups := make([]common.NotificationContainerList, 0)

	notificationContainerList := make(common.NotificationContainerList, 0)
	for _, nc := range n.nextProcessList {
		if len(notificationContainerList) >= notificationBulkLimit {
			bulkGroups = append(bulkGroups, notificationContainerList)
			notificationContainerList = make(common.NotificationContainerList, 0)
		}
		notificationContainerList = append(notificationContainerList, nc)
	}

	if len(notificationContainerList) > 0 {
		bulkGroups = append(bulkGroups, notificationContainerList)
	}

	return bulkGroups
}

func (n *node) notifyBulk() {
	if len(n.nextProcessList) == 0 {
		return
	}

	pushFunc := func(wg *sync.WaitGroup, notificationContainerList common.NotificationContainerList) {
		defer wg.Done()

		if err := n.notify(notificationContainerList); err != nil {
			n.logger.Warn("Bulk notification is failed", zap.Error(err))

			switch et := err.(type) {
			case *common.NotificationError:
				failedList := et.ContainerList()

				if failedList == nil || len(failedList) == 0 {
					break
				}

				failedListMap := make(map[string]bool)
				for _, f := range failedList {
					failedListMap[f.FileItem.Sha512Hex] = true
				}

				failedNotificationContainerList := make(common.NotificationContainerList, 0)

				for _, nc := range notificationContainerList {
					if _, has := failedListMap[nc.FileItem.Sha512Hex]; !has {
						nc.ResponseChan <- true
						continue
					}
					failedNotificationContainerList = append(failedNotificationContainerList, nc)
				}

				n.failureChan <- failedNotificationContainerList
				return
			}
		}

		for _, nc := range notificationContainerList {
			nc.ResponseChan <- true
		}
	}

	bulkGroups := n.createBulkGroup()
	n.nextProcessList = make(map[string]*common.NotificationContainer)

	wg := &sync.WaitGroup{}
	for _, bG := range bulkGroups {
		wg.Add(1)
		go pushFunc(wg, bG)
	}
	wg.Wait()
}

func (n *node) notify(notificationContainerList common.NotificationContainerList) error {
	body, err := json.Marshal(notificationContainerList)
	if err != nil {
		return common.NewNotificationError(notificationContainerList, err)
	}

	req, err := http.NewRequest("POST", fmt.Sprintf("%s%s", n.managerAddr[0], managerEndPoint), bytes.NewBuffer(body))
	if err != nil {
		return common.NewNotificationError(notificationContainerList, err)
	}
	req.Header.Set("X-Action", "notify")
	req.Header.Set("X-Options", n.nodeId)

	res, err := n.client.Do(req)
	if err != nil {
		return common.NewNotificationError(notificationContainerList, err)
	}
	defer func() { _ = res.Body.Close() }()

	if res.StatusCode != 202 {
		if res.StatusCode == 404 {
			return fmt.Errorf("data node is not registered")
		}

		var failedList common.NotificationContainerList
		if err := json.NewDecoder(res.Body).Decode(&failedList); err != nil {
			n.logger.Error("Decoding the response of bulk notify request result is failed", zap.Error(err))
		}

		return common.NewNotificationError(failedList, fmt.Errorf("node manager notify request is failed: %d - %s", res.StatusCode, common.NewErrorFromReader(res.Body).Message))
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

	n.logger.Info(fmt.Sprintf("Data Node is joined to cluster (%s) with node id (%s) as %s", clusterId, nodeId, mode))
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
	n.logger.Info(fmt.Sprintf("Data Node (%s) is marked as %s", n.nodeId, mode))
}

func (n *node) Leave() {
	n.logger.Info(fmt.Sprintf("Data Node is deleted from cluster (%s). Now working as stand-alone", n.clusterId))

	n.clusterId = ""
	n.nodeId = ""
	n.masterAddress = ""
}

func (n *node) Handshake() error {
	req, err := http.NewRequest("POST", fmt.Sprintf("%s%s", n.managerAddr[0], managerEndPoint), nil)
	if err != nil {
		return err
	}
	req.Header.Set("X-Action", "handshake")
	req.Header.Set("X-Options", fmt.Sprintf("%s,%s,%s", strconv.FormatUint(n.nodeSize, 10), n.hardwareAddr, n.bindAddr))

	res, err := n.client.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = res.Body.Close() }()

	if res.StatusCode != 200 {
		if res.StatusCode == 404 {
			return fmt.Errorf("data node is not registered")
		}
		return fmt.Errorf("node manager request is failed (Handshake): %d - %s", res.StatusCode, common.NewErrorFromReader(res.Body).Message)
	}

	initialHandshake := len(n.clusterId) == 0 && len(n.nodeId) == 0

	n.clusterId = res.Header.Get("X-Cluster-Id")
	n.nodeId = res.Header.Get("X-Node-Id")
	if len(n.clusterId) == 0 || len(n.nodeId) == 0 {
		return fmt.Errorf("node manager response wrong for handshake")
	}
	n.masterAddress = res.Header.Get("X-Master")

	if !initialHandshake {
		n.Mode(len(n.masterAddress) == 0)
	}

	return nil
}

func (n *node) Notify(sha512Hex string, usage uint16, size uint32, shadow bool, create bool) <-chan bool {
	responseChan := make(chan bool, 1)

	n.notificationChan <- common.NotificationContainer{
		Create: create,
		FileItem: common.SyncFileItem{
			Sha512Hex: sha512Hex,
			Usage:     usage,
			Size:      size,
			Shadow:    shadow,
		},
		ResponseChan: responseChan,
	}

	return responseChan
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

func (n *node) HardwareAddr() string {
	return n.hardwareAddr
}

func (n *node) BindAddr() string {
	return n.bindAddr
}

func (n *node) NodeSize() uint64 {
	return n.nodeSize
}

var _ Node = &node{}
