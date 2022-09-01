package routing

import (
	"net/http"

	"github.com/freakmaxi/kertish-dfs/manager-node/manager"
	"go.uber.org/zap"
)

type nodeRouter struct {
	manager manager.Node
	logger  *zap.Logger

	definitions []*Definition
}

func NewNodeRouter(nodeManager manager.Node, logger *zap.Logger) Router {
	pR := &nodeRouter{
		manager:     nodeManager,
		logger:      logger,
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
	defer func() { _ = r.Body.Close() }()

	switch r.Method {
	case http.MethodPost:
		n.handlePost(w, r)
	default:
		w.WriteHeader(406)
	}
}

var _ Router = &nodeRouter{}
