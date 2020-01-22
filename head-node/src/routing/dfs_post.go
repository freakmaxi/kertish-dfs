package routing

import (
	"fmt"
	"net/http"
	"os"
	"strings"

	"github.com/freakmaxi/kertish-dfs/head-node/src/common"
	"github.com/freakmaxi/kertish-dfs/head-node/src/errors"
)

func (d *dfsRouter) handlePost(w http.ResponseWriter, r *http.Request) {
	applyTo := r.Header.Get("X-ApplyTo")
	requestedPath := r.Header.Get("X-Path")

	if !d.validateApplyTo(applyTo) || !common.ValidatePath(requestedPath) {
		w.WriteHeader(422)
		return
	}

	switch applyTo {
	case "folder":
		if err := d.dfs.CreateFolder(requestedPath); err != nil {
			if err == os.ErrExist {
				w.WriteHeader(409)
				return
			} else {
				w.WriteHeader(500)
			}
			fmt.Printf("ERROR: Post request for %s (%s) is failed. %s\n", requestedPath, applyTo, err.Error())
			return
		}
	case "file":
		allowEmptyHeader := strings.ToLower(r.Header.Get("X-Overwrite"))
		allowEmpty := len(allowEmptyHeader) > 0 && (strings.Compare(allowEmptyHeader, "1") == 0 || strings.Compare(allowEmptyHeader, "true") == 0)

		contentType := r.Header.Get("Content-Type")
		if len(contentType) == 0 {
			w.WriteHeader(422)
			return
		}

		contentLength := r.ContentLength

		if !allowEmpty && contentLength == -1 {
			w.WriteHeader(411)
			return
		}

		if contentLength == -1 {
			contentLength = 0
		}

		overwriteHeader := strings.ToLower(r.Header.Get("X-Overwrite"))
		overwrite := len(overwriteHeader) > 0 && (strings.Compare(overwriteHeader, "1") == 0 || strings.Compare(overwriteHeader, "true") == 0)

		if err := d.dfs.CreateFile(requestedPath, contentType, uint64(contentLength), r.Body, overwrite); err != nil {
			if err == os.ErrExist {
				w.WriteHeader(409)
				return
			} else if err == errors.ErrNoSpace {
				w.WriteHeader(507)
			} else if err == os.ErrInvalid {
				w.WriteHeader(422)
			} else {
				w.WriteHeader(500)
			}
			fmt.Printf("ERROR: Post request for %s (%s) is failed. %s\n", requestedPath, applyTo, err.Error())
			return
		}
	}

	w.WriteHeader(202)
}
