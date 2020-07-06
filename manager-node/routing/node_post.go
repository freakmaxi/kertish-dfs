package routing

import (
	"encoding/json"
	"io/ioutil"
	"net/http"
	"os"
	"strconv"
	"strings"

	"github.com/freakmaxi/kertish-dfs/basics/common"
	"github.com/freakmaxi/kertish-dfs/basics/errors"
	"go.uber.org/zap"
)

func (n *nodeRouter) handlePost(w http.ResponseWriter, r *http.Request) {
	action := r.Header.Get("X-Action")

	if !n.validatePostAction(action) {
		w.WriteHeader(422)
		return
	}

	switch action {
	case "handshake":
		n.handleHandshake(w, r)
	case "notify":
		n.handleNotify(w, r)
	default:
		w.WriteHeader(406)
	}
}

func (n *nodeRouter) handleHandshake(w http.ResponseWriter, r *http.Request) {
	opts := r.Header.Get("X-Options")
	size, nodeHardwareAddr, nodeAddress, err := n.describeHandshakeOptions(opts)
	if err != nil {
		w.WriteHeader(422)
		return
	}

	clusterId, nodeId, syncSourceNodeAddr, err := n.manager.Handshake(nodeHardwareAddr, nodeAddress, size)
	if err != nil {
		if err == errors.ErrNotFound {
			w.WriteHeader(404)
		} else {
			w.WriteHeader(500)
			n.logger.Error("Node handshake request is failed", zap.Error(err))
		}
		return
	}

	w.Header().Set("X-Cluster-Id", clusterId)
	w.Header().Set("X-Node-Id", nodeId)
	w.Header().Set("X-Master", syncSourceNodeAddr)
}

func (n *nodeRouter) handleNotify(w http.ResponseWriter, r *http.Request) {
	nodeId, notificationContainerList, err := n.describeNotifyOptions(r)
	if err != nil {
		w.WriteHeader(422)
		return
	}

	if err := n.manager.Notify(nodeId, notificationContainerList); err != nil {
		notificationError := err.(*common.NotificationError)

		if notificationError.Is(errors.ErrNotFound) {
			w.WriteHeader(404)
		} else {
			w.WriteHeader(500)
			n.logger.Error("Node sync create request is failed", zap.Error(err))

			if err := json.NewEncoder(w).Encode(notificationError.ContainerList()); err != nil {
				n.logger.Error("Response of bulk notify request result is failed", zap.Error(err))
			}
		}
		return
	}

	w.WriteHeader(202)
}

func (n *nodeRouter) validatePostAction(action string) bool {
	switch action {
	case "handshake", "notify":
		return true
	}
	return false
}

func (n *nodeRouter) describeHandshakeOptions(options string) (uint64, string, string, error) {
	opts := strings.Split(options, ",")
	if len(opts) != 3 {
		return 0, "", "", os.ErrInvalid
	}

	size, err := strconv.ParseUint(opts[0], 10, 64)
	if err != nil {
		return 0, "", "", os.ErrInvalid
	}

	return size, opts[1], opts[2], nil
}

func (n *nodeRouter) describeNotifyOptions(r *http.Request) (string, common.NotificationContainerList, error) {
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		return "", nil, err
	}

	nodeId := r.Header.Get("X-Options")
	notificationContainerList := make(common.NotificationContainerList, 0)
	if err := json.Unmarshal(body, &notificationContainerList); err != nil {
		return "", nil, err
	}

	if len(nodeId) == 0 || len(notificationContainerList) == 0 {
		return "", nil, os.ErrInvalid
	}

	for _, notificationContainer := range notificationContainerList {
		if len(notificationContainer.FileItem.Sha512Hex) != 64 {
			return "", nil, os.ErrInvalid
		}
	}

	return nodeId, notificationContainerList, nil
}
