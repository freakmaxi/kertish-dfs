package routing

import (
	"fmt"
	"net/http"

	"github.com/freakmaxi/kertish-dfs/head-node/src/manager"
)

type dfsRouter struct {
	dfs manager.Dfs

	definitions []*Definition
}

func NewDfsRouter(dfs manager.Dfs) Router {
	pR := &dfsRouter{
		dfs:         dfs,
		definitions: make([]*Definition, 0),
	}
	pR.setup()

	return pR
}

func (d *dfsRouter) setup() {
	d.definitions =
		append(d.definitions,
			&Definition{
				Path:    "/client/dfs",
				Handler: d.manipulate,
			},
		)
}

func (d *dfsRouter) Get() []*Definition {
	return d.definitions
}

func (d *dfsRouter) manipulate(w http.ResponseWriter, r *http.Request) {
	defer func() {
		err := r.Body.Close()
		if err != nil {
			fmt.Printf("ERROR: Request body close is failed. %s\n", err.Error())
		}
	}()

	switch r.Method {
	case "GET":
		d.handleGet(w, r)
	case "POST":
		d.handlePost(w, r)
	case "PUT":
		d.handlePut(w, r)
	case "DELETE":
		d.handleDelete(w, r)
	default:
		w.WriteHeader(406)
	}
}

func (d *dfsRouter) validateApplyTo(applyTo string) bool {
	switch applyTo {
	case "folder", "file":
		return true
	}
	return false
}

var _ Router = &dfsRouter{}
