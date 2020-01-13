package routing

import (
	"fmt"
	"net/http"
	"os"
	"strings"

	"github.com/freakmaxi/2020-dfs/head-node/src/common"
)

func (d *dfsRouter) handlePut(w http.ResponseWriter, r *http.Request) {
	requestedPath := r.Header.Get("X-Path")
	targetPath, targetAction, err := d.describeTarget(r.Header.Get("X-Target"))

	if err != nil || !common.ValidatePath(requestedPath) {
		w.WriteHeader(422)
		return
	}

	switch targetAction {
	case "m":
		if err := d.dfs.Move(requestedPath, targetPath); err != nil {
			if err == os.ErrNotExist {
				w.WriteHeader(404)
				return
			} else if err == os.ErrExist {
				w.WriteHeader(409)
				return
			} else {
				w.WriteHeader(500)
			}
			fmt.Printf("ERROR: Put request for source: %s and target: %s is failed. %s\n", requestedPath, targetPath, err.Error())
		}
	case "c":
		if err := d.dfs.Copy(requestedPath, targetPath); err != nil {
			if err == os.ErrNotExist {
				w.WriteHeader(404)
				return
			} else if err == os.ErrExist {
				w.WriteHeader(409)
				return
			} else {
				w.WriteHeader(500)
			}
			fmt.Printf("ERROR: Put request for source: %s and target: %s is failed. %s\n", requestedPath, targetPath, err.Error())
		}
	}
}

func (d *dfsRouter) describeTarget(target string) (string, string, error) {
	commaIdx := strings.Index(target, ",")
	if commaIdx == -1 {
		return "", "", os.ErrInvalid
	}

	action := strings.ToLower(target[:commaIdx])
	targetPath := target[commaIdx+1:]

	if !common.ValidatePath(targetPath) {
		return "", "", os.ErrInvalid
	}

	switch action {
	case "m", "c":
		return targetPath, action, nil
	}

	return "", "", os.ErrInvalid
}
