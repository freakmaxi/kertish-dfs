package routing

import (
	"fmt"
	"net/http"
	"os"
)

func (d *dfsRouter) handleDelete(w http.ResponseWriter, r *http.Request) {
	requestedPaths, _, err := d.describeXPath(r.Header.Get("X-Path"))
	if err != nil || len(requestedPaths) > 1 {
		w.WriteHeader(422)
		return
	}

	if err := d.dfs.Delete(requestedPaths[0]); err != nil {
		if err == os.ErrNotExist {
			w.WriteHeader(404)
			return
		} else {
			w.WriteHeader(500)
		}
		fmt.Printf("ERROR: Delete request for %s is failed. %s\n", requestedPaths[0], err.Error())
	}
}
