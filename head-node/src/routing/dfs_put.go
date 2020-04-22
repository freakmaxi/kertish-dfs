package routing

import (
	"fmt"
	"net/http"
	"net/url"
	"os"
	"strings"

	"github.com/freakmaxi/kertish-dfs/basics/common"
	"github.com/freakmaxi/kertish-dfs/basics/errors"
)

func (d *dfsRouter) handlePut(w http.ResponseWriter, r *http.Request) {
	requestedPaths, sourceAction, err := d.describeXPath(r.Header.Get("X-Path"))
	if err != nil {
		w.WriteHeader(422)
		return
	}

	targetPath, targetAction, err := d.describeTarget(r.Header.Get("X-Target"))
	if err != nil {
		w.WriteHeader(422)
		return
	}

	overwriteHeader := strings.ToLower(r.Header.Get("X-Overwrite"))
	overwrite := len(overwriteHeader) > 0 && (strings.Compare(overwriteHeader, "1") == 0 || strings.Compare(overwriteHeader, "true") == 0)
	join := strings.Compare(sourceAction, "j") == 0

	switch targetAction {
	case "m":
		if err := d.dfs.Move(requestedPaths, targetPath, join, overwrite); err != nil {
			if err == os.ErrNotExist {
				w.WriteHeader(404)
				return
			} else if err == os.ErrExist {
				w.WriteHeader(409)
				return
			} else if err == errors.ErrJoinConflict {
				w.WriteHeader(412)
				return
			} else if err == os.ErrInvalid {
				w.WriteHeader(422)
				return
			} else {
				w.WriteHeader(500)
			}
			fmt.Printf("ERROR: Put request for source: %s and target: %s is failed. %s\n", strings.Join(requestedPaths, " "), targetPath, err.Error())
		}
	case "c":
		if err := d.dfs.Copy(requestedPaths, targetPath, join, overwrite); err != nil {
			if err == os.ErrNotExist {
				w.WriteHeader(404)
				return
			} else if err == os.ErrExist {
				w.WriteHeader(409)
				return
			} else if err == errors.ErrJoinConflict {
				w.WriteHeader(412)
				return
			} else if err == os.ErrInvalid {
				w.WriteHeader(422)
				return
			} else {
				w.WriteHeader(500)
			}
			fmt.Printf("ERROR: Put request for source: %s and target: %s is failed. %s\n", strings.Join(requestedPaths, " "), targetPath, err.Error())
		}
	}
}

func (d *dfsRouter) describeTarget(target string) (string, string, error) {
	commaIdx := strings.Index(target, ",")
	if commaIdx == -1 {
		return "", "", os.ErrInvalid
	}

	action := strings.ToLower(target[:commaIdx])
	targetPath, err := url.QueryUnescape(target[commaIdx+1:])

	if err != nil || !common.ValidatePath(targetPath) {
		return "", "", os.ErrInvalid
	}

	switch action {
	case "m", "c":
		return targetPath, action, nil
	}

	return "", "", os.ErrInvalid
}
