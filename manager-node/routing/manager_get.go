package routing

import (
	"encoding/json"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/freakmaxi/kertish-dfs/basics/common"
	"github.com/freakmaxi/kertish-dfs/basics/errors"
	"github.com/freakmaxi/kertish-dfs/manager-node/manager"
	"go.uber.org/zap"
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
	case "repair":
		m.handleRepairConsistency(w, r)
	case "health":
		m.handleHealth(w, r)
	case "move":
		m.handleMove(w, r)
	case "balance":
		m.handleBalance(w, r)
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
		if errSync := m.synchronize.QueueClusters(); errSync != nil {
			err = errors.ErrSync
		}
	} else {
		m.synchronize.QueueCluster(clusterId, false, false)
	}

	if err == nil {
		w.WriteHeader(202)
		return
	}

	if err == errors.ErrNotFound {
		w.WriteHeader(404)
	} else {
		w.WriteHeader(500)
		m.logger.Error("Sync request is failed", zap.Error(err))
	}

	e := common.NewError(100, err.Error())
	if err := json.NewEncoder(w).Encode(e); err != nil {
		m.logger.Error("Response of sync request is failed", zap.Error(err))
	}
}

func (m *managerRouter) handleRepairConsistency(w http.ResponseWriter, r *http.Request) {
	repairOption := r.Header.Get("X-Options")
	repairOption = strings.ToLower(repairOption)

	var repairType manager.RepairType
	switch repairOption {
	case "structure":
		repairType = manager.RTStructureL1
	case "structure+integrity":
		repairType = manager.RTStructureL2
	case "integrity":
		repairType = manager.RTIntegrityL1
	case "integrity+checksum":
		repairType = manager.RTIntegrityL2
	case "checksum":
		repairType = manager.RTChecksumL1
	case "checksum+rebuild":
		repairType = manager.RTChecksumL2
	default:
		repairType = manager.RTFull
	}

	err := m.repair.Start(repairType)
	if err == nil {
		w.WriteHeader(202)
		return
	}

	if err == errors.ErrNotFound {
		w.WriteHeader(404)
	} else if err == errors.ErrProcessing {
		w.WriteHeader(423)
	} else {
		w.WriteHeader(500)
		m.logger.Error("Repair request is failed", zap.String("option", repairOption), zap.Error(err))
	}

	e := common.NewError(105, err.Error())
	if err := json.NewEncoder(w).Encode(e); err != nil {
		m.logger.Error("Response of repair request is failed", zap.Error(err))
	}
}

func (m *managerRouter) handleHealth(w http.ResponseWriter, r *http.Request) {
	report, err := m.health.Report()
	if err == nil {
		if err := json.NewEncoder(w).Encode(report); err != nil {
			m.logger.Error("Response of health report request result is failed", zap.Error(err))
		}
		return
	}

	w.WriteHeader(500)
	m.logger.Error("Health report request is failed", zap.Error(err))

	e := common.NewError(140, err.Error())
	if err := json.NewEncoder(w).Encode(e); err != nil {
		m.logger.Error("Response of health report request is failed", zap.Error(err))
	}
}

func (m *managerRouter) handleMove(w http.ResponseWriter, r *http.Request) {
	sourceClusterId, targetClusterId, valid := m.describeMoveOptions(r.Header.Get("X-Options"))
	if !valid {
		w.WriteHeader(422)
		return
	}

	if err := m.manager.MoveCluster(sourceClusterId, targetClusterId); err != nil {
		if err == errors.ErrNotFound {
			w.WriteHeader(404)
		} else if err == errors.ErrNotAvailableForClusterAction {
			w.WriteHeader(503)
		} else if err == errors.ErrNoSpace {
			w.WriteHeader(507)
		} else {
			w.WriteHeader(500)
			m.logger.Error(
				"Move request is failed",
				zap.String("sourceClusterId", sourceClusterId),
				zap.String("targetClusterId", targetClusterId),
				zap.Error(err),
			)
		}

		e := common.NewError(130, err.Error())
		if err := json.NewEncoder(w).Encode(e); err != nil {
			m.logger.Error("Response of move request is failed", zap.Error(err))
		}
	}
}

func (m *managerRouter) handleBalance(w http.ResponseWriter, r *http.Request) {
	clusterIds, valid := m.describeBalanceOptions(r.Header.Get("X-Options"))
	if !valid {
		w.WriteHeader(422)
		return
	}

	if err := m.manager.BalanceClusters(clusterIds); err != nil {
		if err == errors.ErrNotFound {
			w.WriteHeader(404)
		} else if err == errors.ErrNotAvailableForClusterAction {
			w.WriteHeader(503)
		} else {
			w.WriteHeader(500)
			m.logger.Error("Balance request is failed", zap.Strings("clusterIds", clusterIds), zap.Error(err))
		}

		e := common.NewError(135, err.Error())
		if err := json.NewEncoder(w).Encode(e); err != nil {
			m.logger.Error("Response of balance request is failed", zap.Error(err))
		}
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
		repairStatus := m.repair.Status()

		w.Header().Add("X-Repairing", strconv.FormatBool(repairStatus.Processing))
		if repairStatus.Timestamp != nil {
			w.Header().Add("X-Repairing-Timestamp", repairStatus.Timestamp.Format(time.RFC3339))
		}

		if err := json.NewEncoder(w).Encode(clusters); err != nil {
			m.logger.Error("Response of get clusters request is failed", zap.String("clusterId", clusterId), zap.Error(err))
		}
		return
	}

	if err == errors.ErrNotFound {
		w.WriteHeader(404)
	} else {
		w.WriteHeader(500)
		m.logger.Error("Get clusters request is failed", zap.String("clusterId", clusterId), zap.Error(err))
	}

	e := common.NewError(110, err.Error())
	if err := json.NewEncoder(w).Encode(e); err != nil {
		m.logger.Error("Response of get clusters request is failed", zap.Error(err))
	}
}

func (m *managerRouter) handleFind(w http.ResponseWriter, r *http.Request) {
	sha512Hex := r.Header.Get("X-Options")

	clusterId, addresses, err := m.manager.Find(sha512Hex, common.MTCreate)

	if err == nil {
		w.Header().Set("X-Cluster-Id", clusterId)
		w.Header().Set("X-Address", addresses[0])
		return
	}

	if err == os.ErrNotExist {
		w.WriteHeader(404)
	} else if err == errors.ErrNoAvailableClusterNode || err == errors.ErrNoAvailableActionNode {
		w.WriteHeader(503)
	} else {
		w.WriteHeader(500)
		m.logger.Error("Find request is failed", zap.Error(err))
	}

	e := common.NewError(120, err.Error())
	if err := json.NewEncoder(w).Encode(e); err != nil {
		m.logger.Error("Response of find request is failed", zap.Error(err))
	}
}

func (m *managerRouter) validateGetAction(action string) bool {
	switch action {
	case "sync", "repair", "health", "move", "balance", "clusters", "find":
		return true
	}
	return false
}

func (m *managerRouter) describeMoveOptions(options string) (string, string, bool) {
	sourceClusterId := ""
	targetClusterId := ""

	commaIdx := strings.Index(options, ",")
	if commaIdx > -1 {
		sourceClusterId = options[:commaIdx]
		targetClusterId = options[commaIdx+1:]
	}

	return sourceClusterId, targetClusterId, len(sourceClusterId) > 0 && len(targetClusterId) > 0
}

func (m *managerRouter) describeBalanceOptions(options string) ([]string, bool) {
	clusterIds := make([]string, 0)

	for len(options) > 0 {
		commaIdx := strings.Index(options, ",")
		if commaIdx == -1 {
			if len(options) > 0 {
				clusterIds = append(clusterIds, options)
			}
			break
		}

		clusterId := options[:commaIdx]
		if len(clusterId) > 0 {
			clusterIds = append(clusterIds, clusterId)
		}
		options = options[commaIdx+1:]
	}

	if len(clusterIds) == 1 {
		return nil, false
	}
	return clusterIds, true
}
