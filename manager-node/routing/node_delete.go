package routing

import (
	"encoding/json"
	"io/ioutil"
	"net/http"
	"os"

	"github.com/freakmaxi/kertish-dfs/basics/common"
	"github.com/freakmaxi/kertish-dfs/basics/errors"
	"go.uber.org/zap"
)

func (n *nodeRouter) handleDelete(w http.ResponseWriter, r *http.Request) {
	action := r.Header.Get("X-Action")

	if !n.validateDeleteAction(action) {
		w.WriteHeader(422)
		return
	}

	switch action {
	case "delete":
		n.handleSyncDelete(w, r)
	default:
		w.WriteHeader(406)
	}
}

func (n *nodeRouter) handleSyncDelete(w http.ResponseWriter, r *http.Request) {
	nodeId, fileItemList, err := n.describeDeleteOptions(r)
	if err != nil {
		w.WriteHeader(422)
		return
	}

	if err := n.manager.Notify(nodeId, fileItemList, false); err != nil {
		if err == errors.ErrNotFound {
			w.WriteHeader(404)
		} else {
			w.WriteHeader(500)
			n.logger.Error("Node sync delete request is failed", zap.Error(err))
		}
	}
}

func (n *nodeRouter) validateDeleteAction(action string) bool {
	switch action {
	case "delete":
		return true
	}
	return false
}

func (n *nodeRouter) describeDeleteOptions(r *http.Request) (string, common.SyncFileItemList, error) {
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		return "", nil, err
	}

	nodeId := r.Header.Get("X-Options")
	fileItemList := make(common.SyncFileItemList, 0)
	if err := json.Unmarshal(body, &fileItemList); err != nil {
		return "", nil, err
	}

	if len(nodeId) == 0 || len(fileItemList) == 0 {
		return "", nil, os.ErrInvalid
	}

	for _, syncFileItem := range fileItemList {
		if len(syncFileItem.Sha512Hex) != 64 {
			return "", nil, os.ErrInvalid
		}
	}

	return nodeId, fileItemList, nil
}
