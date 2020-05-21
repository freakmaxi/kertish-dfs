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
	case "create":
		n.handleSyncCreate(w, r)
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
		}
		return
	}

	w.Header().Set("X-Cluster-Id", clusterId)
	w.Header().Set("X-Node-Id", nodeId)
	w.Header().Set("X-Master", syncSourceNodeAddr)
}

func (n *nodeRouter) handleSyncCreate(w http.ResponseWriter, r *http.Request) {
	nodeId, fileItemList, err := n.describeCreateOptions(r)
	if err != nil {
		w.WriteHeader(422)
		return
	}

	if err := n.manager.Create(nodeId, fileItemList); err != nil {
		if err == errors.ErrNotFound {
			w.WriteHeader(404)
		} else {
			w.WriteHeader(500)
		}
		return
	}

	w.WriteHeader(202)
}

func (n *nodeRouter) validatePostAction(action string) bool {
	switch action {
	case "handshake", "create":
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

func (n *nodeRouter) describeCreateOptions(r *http.Request) (string, common.SyncFileItems, error) {
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		return "", nil, err
	}

	nodeId := r.Header.Get("X-Options")
	fileItemList := make(common.SyncFileItems, 0)
	if err := json.Unmarshal(body, &fileItemList); err != nil {
		return "", nil, err
	}

	if len(nodeId) == 0 || len(fileItemList) == 0 {
		return "", nil, os.ErrInvalid
	}

	for _, fileItem := range fileItemList {
		if len(fileItem.Sha512Hex) != 64 {
			return "", nil, os.ErrInvalid
		}
	}

	return nodeId, fileItemList, nil
}
