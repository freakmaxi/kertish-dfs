package routing

import (
	"net/http"

	"github.com/freakmaxi/kertish-dfs/manager-node/manager"
)

type nodeRouter struct {
	manager manager.Node

	definitions []*Definition
}

func NewNodeRouter(nodeManager manager.Node) Router {
	pR := &nodeRouter{
		manager:     nodeManager,
		definitions: make([]*Definition, 0),
	}
	pR.setup()

	return pR
}

func (n *nodeRouter) setup() {
	n.definitions =
		append(n.definitions,
			&Definition{
				Path:    "/client/node",
				Handler: n.manipulate,
			},
		)
}

func (n *nodeRouter) Get() []*Definition {
	return n.definitions
}

func (n *nodeRouter) manipulate(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()

	switch r.Method {
	case "POST":
		n.handlePost(w, r)
	case "DELETE":
		n.handleDelete(w, r)
	default:
		w.WriteHeader(406)
	}
}

var _ Router = &nodeRouter{}
