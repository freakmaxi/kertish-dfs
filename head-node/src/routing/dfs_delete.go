package routing

import (
	"fmt"
	"net/http"
	"os"

	"github.com/freakmaxi/kertish-dfs/head-node/src/common"
)

func (d *dfsRouter) handleDelete(w http.ResponseWriter, r *http.Request) {
	requestedPath := r.Header.Get("X-Path")

	if !common.ValidatePath(requestedPath) {
		w.WriteHeader(422)
		return
	}

	if err := d.dfs.Delete(requestedPath); err != nil {
		if err == os.ErrNotExist {
			w.WriteHeader(404)
			return
		} else {
			w.WriteHeader(500)
		}
		fmt.Printf("ERROR: Delete request for %s is failed. %s\n", requestedPath, err.Error())
	}
}
