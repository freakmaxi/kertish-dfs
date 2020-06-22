package routing

import (
	"net/http"
	"net/url"
	"os"
	"strings"

	"github.com/freakmaxi/kertish-dfs/basics/common"
	"github.com/freakmaxi/kertish-dfs/head-node/manager"
	"go.uber.org/zap"
)

type dfsRouter struct {
	dfs    manager.Dfs
	logger *zap.Logger

	definitions []*Definition
}

func NewDfsRouter(dfs manager.Dfs, logger *zap.Logger) Router {
	pR := &dfsRouter{
		dfs:         dfs,
		logger:      logger,
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
	defer func() { _ = r.Body.Close() }()

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

func (d *dfsRouter) describeXPath(xPath string) ([]string, string, error) {
	action := ""
	commaIdx := strings.Index(xPath, ",")
	if commaIdx == 1 {
		action = xPath[:1]
		xPath = xPath[2:]
	}

	switch action {
	case "", "j":
	default:
		return nil, "", os.ErrInvalid
	}

	paths := strings.Split(xPath, ",")
	for i := range paths {
		p, err := url.QueryUnescape(paths[i])
		if err != nil {
			return nil, "", err
		}
		if !common.ValidatePath(p) {
			return nil, "", os.ErrInvalid
		}
		paths[i] = p
	}

	if len(paths) == 0 {
		return nil, "", os.ErrInvalid
	}

	return paths, action, nil
}

var _ Router = &dfsRouter{}
