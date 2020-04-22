package routing

import (
	"fmt"
	"net/http"

	"github.com/freakmaxi/kertish-dfs/manager-node/manager"
)

type managerRouter struct {
	manager manager.Cluster

	definitions []*Definition
}

func NewManagerRouter(clusterManager manager.Cluster) Router {
	pR := &managerRouter{
		manager:     clusterManager,
		definitions: make([]*Definition, 0),
	}
	pR.setup()

	return pR
}

func (m *managerRouter) setup() {
	m.definitions =
		append(m.definitions,
			&Definition{
				Path:    "/client/manager",
				Handler: m.manipulate,
			},
		)
}

func (m *managerRouter) Get() []*Definition {
	return m.definitions
}

func (m *managerRouter) manipulate(w http.ResponseWriter, r *http.Request) {
	defer func() {
		err := r.Body.Close()
		if err != nil {
			fmt.Printf("ERROR: Request body close is failed. %s\n", err.Error())
		}
	}()

	switch r.Method {
	case "POST":
		m.handlePost(w, r)
	case "DELETE":
		m.handleDelete(w, r)
	case "GET":
		m.handleGet(w, r)
	default:
		w.WriteHeader(406)
	}
}

var _ Router = &managerRouter{}
