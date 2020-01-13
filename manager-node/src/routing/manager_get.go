package routing

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/freakmaxi/2020-dfs/manager-node/src/common"
	"github.com/freakmaxi/2020-dfs/manager-node/src/errors"
)

func (m *managerRouter) handleGet(w http.ResponseWriter, r *http.Request) {
	action := r.Header.Get("X-Action")

	if !m.validateGetAction(action) {
		w.WriteHeader(422)
		return
	}

	switch action {
	case "sync":
		m.handleSync(w, r)
	case "clusters":
		m.handleClusters(w, r)
	case "find":
		m.handleFind(w, r)
	default:
		w.WriteHeader(406)
	}
}

func (m *managerRouter) handleSync(w http.ResponseWriter, r *http.Request) {
	clusterId := r.Header.Get("X-Options")

	var err error
	if len(clusterId) == 0 {
		err = m.manager.SyncClusters()
	} else {
		err = m.manager.SyncCluster(clusterId)
	}

	if err == nil {
		return
	}

	if err == errors.ErrNotFound {
		w.WriteHeader(404)
	} else {
		w.WriteHeader(500)
	}
	e := common.NewError(100, err.Error())
	if err := json.NewEncoder(w).Encode(e); err != nil {
		fmt.Printf("ERROR: Get request is failed. %s\n", err.Error())
	}
}

func (m *managerRouter) handleClusters(w http.ResponseWriter, r *http.Request) {
	clusterId := r.Header.Get("X-Options")

	var clusters common.Clusters
	var err error
	if len(clusterId) == 0 {
		clusters, err = m.manager.GetClusters()
	} else {
		cluster, e := m.manager.GetCluster(clusterId)
		if e == nil {
			clusters = common.Clusters{cluster}
		}
		err = e
	}

	if err == nil {
		if err := json.NewEncoder(w).Encode(clusters); err != nil {
			fmt.Printf("ERROR: Get request is failed. %s\n", err.Error())
		}
		return
	}

	if err == errors.ErrNotFound {
		w.WriteHeader(404)
	} else {
		w.WriteHeader(500)
	}
	e := common.NewError(110, err.Error())
	if err := json.NewEncoder(w).Encode(e); err != nil {
		fmt.Printf("ERROR: Get request is failed. %s\n", err.Error())
	}
}

func (m *managerRouter) handleFind(w http.ResponseWriter, r *http.Request) {
	sha512Hex := r.Header.Get("X-Options")

	clusterId, address, err := m.manager.Find(sha512Hex, true)

	if err == nil {
		w.Header().Set("X-ClusterId", clusterId)
		w.Header().Set("X-Address", address)
		return
	}

	if err == errors.ErrNotFound {
		w.WriteHeader(404)
	} else {
		w.WriteHeader(500)
	}
	e := common.NewError(120, err.Error())
	if err := json.NewEncoder(w).Encode(e); err != nil {
		fmt.Printf("ERROR: Get request is failed. %s\n", err.Error())
	}
}

func (m *managerRouter) validateGetAction(action string) bool {
	switch action {
	case "sync", "clusters", "find":
		return true
	}
	return false
}
