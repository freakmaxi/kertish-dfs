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

type hookRouter struct {
	hook   manager.Hook
	logger *zap.Logger

	definitions []*Definition
}

func NewHookRouter(hook manager.Hook, logger *zap.Logger) Router {
	pR := &hookRouter{
		hook:        hook,
		logger:      logger,
		definitions: make([]*Definition, 0),
	}
	pR.setup()

	return pR
}

func (h *hookRouter) setup() {
	h.definitions =
		append(h.definitions,
			&Definition{
				Path:    "/client/hook",
				Handler: h.manipulate,
			},
		)
}

func (h *hookRouter) Get() []*Definition {
	return h.definitions
}

func (h *hookRouter) manipulate(w http.ResponseWriter, r *http.Request) {
	defer func() { _ = r.Body.Close() }()

	switch r.Method {
	case http.MethodGet:
		h.handleGet(w, r)
	case http.MethodPost:
		h.handlePost(w, r)
	case http.MethodDelete:
		h.handleDelete(w, r)
	default:
		w.WriteHeader(406)
	}
}

func (h *hookRouter) describeXPath(xPath string) ([]string, error) {
	paths := strings.Split(xPath, ",")
	for i := range paths {
		p, err := url.QueryUnescape(paths[i])
		if err != nil {
			return nil, err
		}
		if !common.ValidatePath(p) {
			return nil, os.ErrInvalid
		}
		paths[i] = p
	}

	if len(paths) == 0 {
		return nil, os.ErrInvalid
	}

	return paths, nil
}

var _ Router = &hookRouter{}
