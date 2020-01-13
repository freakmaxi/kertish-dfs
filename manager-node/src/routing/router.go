package routing

import (
	"net/http"

	"github.com/gorilla/mux"
)

type Router interface {
	Get() []*Definition
}

type Definition struct {
	Path    string
	Handler func(http.ResponseWriter, *http.Request)
}

type Manager struct {
	mux *mux.Router
}

func NewManager() *Manager {
	m := mux.NewRouter()

	return &Manager{
		mux: m,
	}
}

func (m *Manager) Add(router Router) {
	for _, d := range router.Get() {
		m.mux.HandleFunc(d.Path, d.Handler)
	}
}

func (m *Manager) Get() *mux.Router {
	return m.mux
}
