package routing

import (
	"net/http"
	"os"
	"strconv"
	"strings"

	"github.com/freakmaxi/kertish-dfs/basics/errors"
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
	opts := r.Header.Get("X-Options")
	nodeId, sha512Hex, shadow, size, err := n.describeDeleteOptions(opts)
	if err != nil {
		w.WriteHeader(422)
		return
	}

	if err := n.manager.Delete(nodeId, sha512Hex, shadow, size); err != nil {
		if err == errors.ErrNotFound {
			w.WriteHeader(404)
		} else {
			w.WriteHeader(500)
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

func (n *nodeRouter) describeDeleteOptions(options string) (string, string, bool, uint64, error) {
	opts := strings.Split(options, ",")
	if len(opts) != 4 {
		return "", "", false, 0, os.ErrInvalid
	}

	nodeId := opts[0]
	fileId := opts[1]
	shadowString := opts[2]
	sizeString := opts[3]

	if len(nodeId) == 0 || len(fileId) == 0 || len(shadowString) == 0 || len(sizeString) == 0 {
		return "", "", false, 0, os.ErrInvalid
	}

	size, err := strconv.ParseUint(sizeString, 10, 64)
	if err != nil {
		return "", "", false, 0, os.ErrInvalid
	}

	return nodeId, fileId, strings.Compare(shadowString, "1") == 0, size, nil
}

var _ Router = &nodeRouter{}
