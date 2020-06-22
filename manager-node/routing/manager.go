package routing

import (
	"net/http"

	"github.com/freakmaxi/kertish-dfs/manager-node/manager"
	"go.uber.org/zap"
)

type managerRouter struct {
	manager     manager.Cluster
	synchronize manager.Synchronize
	repair      manager.Repair
	health      manager.HealthCheck
	logger      *zap.Logger

	definitions []*Definition
}

func NewManagerRouter(clusterManager manager.Cluster, synchronize manager.Synchronize, repair manager.Repair, health manager.HealthCheck, logger *zap.Logger) Router {
	pR := &managerRouter{
		manager:     clusterManager,
		synchronize: synchronize,
		repair:      repair,
		health:      health,
		logger:      logger,
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
	defer func() { _ = r.Body.Close() }()

	switch r.Method {
	case "POST":
		m.handlePost(w, r)
	case "PUT":
		m.handlePut(w, r)
	case "DELETE":
		m.handleDelete(w, r)
	case "GET":
		m.handleGet(w, r)
	default:
		w.WriteHeader(406)
	}
}

var _ Router = &managerRouter{}
