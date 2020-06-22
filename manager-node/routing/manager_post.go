package routing

import (
	"encoding/json"
	"net/http"
	"os"
	"strconv"
	"strings"

	"github.com/freakmaxi/kertish-dfs/basics/common"
	"github.com/freakmaxi/kertish-dfs/basics/errors"
	"go.uber.org/zap"
)

func (m *managerRouter) handlePost(w http.ResponseWriter, r *http.Request) {
	action := r.Header.Get("X-Action")

	if !m.validatePostAction(action) {
		w.WriteHeader(422)
		return
	}

	switch action {
	case "register":
		m.handleRegister(w, r)
	case "snapshot":
		m.handleCreateSnapshot(w, r)
	case "reserve":
		m.handleReserve(w, r)
	case "readMap", "createMap", "deleteMap":
		mapType := common.MT_Read
		switch action {
		case "createMap":
			mapType = common.MT_Create
		case "deleteMap":
			mapType = common.MT_Delete
		}
		m.handleMap(w, r, mapType)
	default:
		w.WriteHeader(406)
	}
}

func (m *managerRouter) handleRegister(w http.ResponseWriter, r *http.Request) {
	clusterId, addresses := m.describeRegisterOptions(r.Header.Get("X-Options"))

	var cluster *common.Cluster
	var err error
	if len(clusterId) == 0 {
		cluster, err = m.manager.Register(addresses)
	} else {
		err = m.manager.RegisterNodesTo(clusterId, addresses)
		if err == nil {
			cluster, err = m.manager.GetCluster(clusterId)
		}
	}

	if err == nil {
		if err := json.NewEncoder(w).Encode(cluster); err != nil {
			m.logger.Error("Response of register cluster request is failed", zap.Error(err))
		}
		return
	}

	if err == errors.ErrRegistered {
		w.WriteHeader(409)
	} else {
		w.WriteHeader(400)
		m.logger.Error(
			"Register cluster request is failed",
			zap.String("clusterId", clusterId),
			zap.Strings("addresses", addresses),
			zap.Error(err),
		)
	}

	e := common.NewError(200, err.Error())
	if err := json.NewEncoder(w).Encode(e); err != nil {
		m.logger.Error("Response of register cluster request is failed", zap.Error(err))
	}
}

func (m *managerRouter) handleCreateSnapshot(w http.ResponseWriter, r *http.Request) {
	clusterId := r.Header.Get("X-Options")
	if len(clusterId) == 0 {
		w.WriteHeader(422)
		return
	}

	err := m.manager.CreateSnapshot(clusterId)
	if err == nil {
		return
	}

	w.WriteHeader(400)
	m.logger.Error("Create snapshot request is failed", zap.String("clusterId", clusterId), zap.Error(err))

	e := common.NewError(205, err.Error())
	if err := json.NewEncoder(w).Encode(e); err != nil {
		m.logger.Error("Response of create snapshot request is failed", zap.Error(err))
	}
}

func (m *managerRouter) handleReserve(w http.ResponseWriter, r *http.Request) {
	size, err := strconv.ParseUint(r.Header.Get("X-Size"), 10, 64)
	if err != nil {
		w.WriteHeader(422)
		return
	}

	reservationMap, err := m.manager.Reserve(size)
	if err == nil {
		if err := json.NewEncoder(w).Encode(reservationMap); err != nil {
			m.logger.Error("Response of reserve request is failed", zap.Error(err))
		}
		return
	}

	if err == errors.ErrNoAvailableClusterNode {
		w.WriteHeader(503)
	} else if err == errors.ErrNoDiskSpace {
		w.WriteHeader(507)
	} else {
		w.WriteHeader(400)
		m.logger.Error("Reserve request is failed", zap.Uint64("size", size), zap.Error(err))
	}

	e := common.NewError(210, err.Error())
	if err := json.NewEncoder(w).Encode(e); err != nil {
		m.logger.Error("Response of reserve request is failed", zap.Error(err))
	}
}

func (m *managerRouter) handleMap(w http.ResponseWriter, r *http.Request, mapType common.MapType) {
	sha512HexList := strings.Split(r.Header.Get("X-Options"), ",")
	if len(sha512HexList) == 0 {
		w.WriteHeader(422)
		return
	}

	clusterMapping, err := m.manager.Map(sha512HexList, mapType)
	if err == nil {
		if err := json.NewEncoder(w).Encode(clusterMapping); err != nil {
			m.logger.Error("Response of map request is failed", zap.Error(err))
		}
		return
	}

	if err == os.ErrNotExist {
		w.WriteHeader(404)
	} else if err == errors.ErrNoAvailableClusterNode || err == errors.ErrNoAvailableActionNode {
		w.WriteHeader(503)
	} else {
		w.WriteHeader(400)
		m.logger.Error("Map request is failed", zap.Strings("sha512HexList", sha512HexList), zap.Error(err))
	}

	e := common.NewError(220, err.Error())
	if err := json.NewEncoder(w).Encode(e); err != nil {
		m.logger.Error("Response of map request is failed", zap.Error(err))
	}
}

func (m *managerRouter) validatePostAction(action string) bool {
	switch action {
	case "register", "snapshot", "reserve", "readMap", "createMap", "deleteMap":
		return true
	}
	return false
}

func (m *managerRouter) describeRegisterOptions(options string) (string, []string) {
	clusterId := ""
	eqIdx := strings.Index(options, "=")
	if eqIdx > -1 {
		clusterId = options[:eqIdx]
		options = options[eqIdx+1:]
	}
	addresses := strings.Split(options, ",")

	return clusterId, addresses
}
