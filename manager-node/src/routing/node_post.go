package routing

import (
	"net/http"
	"os"
	"strconv"
	"strings"

	"github.com/freakmaxi/kertish-dfs/manager-node/src/errors"
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
	opts := r.Header.Get("X-Options")
	nodeId, sha512Hex, err := n.describeCreateOptions(opts)
	if err != nil {
		w.WriteHeader(422)
		return
	}

	if err := n.manager.Create(nodeId, sha512Hex); err != nil {
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

func (n *nodeRouter) describeCreateOptions(options string) (string, string, error) {
	opts := strings.Split(options, ",")
	if len(opts) != 2 {
		return "", "", os.ErrInvalid
	}

	nodeId := opts[0]
	fileId := opts[1]

	if len(nodeId) == 0 || len(fileId) != 64 {
		return "", "", os.ErrInvalid
	}

	return nodeId, fileId, nil
}
