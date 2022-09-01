package routing

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"github.com/freakmaxi/kertish-dfs/basics/common"
	"go.uber.org/zap"
)

type clusterState struct {
	clusterId string
	state     common.States
}

func (m *managerRouter) handlePut(w http.ResponseWriter, r *http.Request) {
	action := r.Header.Get("X-Action")

	if !m.validatePutAction(action) {
		w.WriteHeader(422)
		return
	}

	switch action {
	case "state":
		m.handleState(w, r)
	case "snapshot":
		m.handleRestoreSnapshot(w, r)
	default:
		w.WriteHeader(406)
	}
}

func (m *managerRouter) handleState(w http.ResponseWriter, r *http.Request) {
	clusterStates, err := m.describeStateOptions(r.Header.Get("X-Options"))
	if len(clusterStates) == 0 || err != nil {
		w.WriteHeader(422)
		if err != nil {
			e := common.NewError(300, err.Error())
			if err := json.NewEncoder(w).Encode(e); err != nil {
				m.logger.Error("Response of cluster state change request is failed", zap.Error(err))
			}
		}
	}

	for _, clusterState := range clusterStates {
		if len(clusterState.clusterId) == 0 {
			if err = m.manager.ChangeStateAll(clusterState.state); err != nil {
				w.WriteHeader(400)
				m.logger.Error("Change cluster state request is failed", zap.Error(err))

				e := common.NewError(300, err.Error())
				if err := json.NewEncoder(w).Encode(e); err != nil {
					m.logger.Error("Response of change cluster state request is failed", zap.Error(err))
				}
			}
			break
		}

		err = m.manager.ChangeState(clusterState.clusterId, clusterState.state)
		if err == nil {
			continue
		}

		w.WriteHeader(400)
		m.logger.Error("Change cluster state request is failed", zap.Error(err))

		e := common.NewError(300, err.Error())
		if err := json.NewEncoder(w).Encode(e); err != nil {
			m.logger.Error("Response of change cluster state request is failed", zap.Error(err))
		}
	}
}

func (m *managerRouter) handleRestoreSnapshot(w http.ResponseWriter, r *http.Request) {
	clusterId, snapshotIndex, err := m.describeRestoreSnapshotOptions(r.Header.Get("X-Options"))
	if len(clusterId) == 0 || err != nil {
		w.WriteHeader(422)
		if err != nil {
			e := common.NewError(400, err.Error())
			if err := json.NewEncoder(w).Encode(e); err != nil {
				m.logger.Error("Response of restore snapshot request is failed", zap.Error(err))
			}
		}
	}

	err = m.manager.RestoreSnapshot(clusterId, snapshotIndex)
	if err == nil {
		return
	}

	w.WriteHeader(400)
	m.logger.Error("Restore snapshot request is failed", zap.Error(err))

	e := common.NewError(400, err.Error())
	if err := json.NewEncoder(w).Encode(e); err != nil {
		m.logger.Error("Response of restore snapshot request is failed", zap.Error(err))
	}
}

func (m *managerRouter) validatePutAction(action string) bool {
	switch action {
	case "state", "snapshot":
		return true
	}
	return false
}

func (m *managerRouter) describeStateOptions(options string) ([]*clusterState, error) {
	clusterIdWithStateList := strings.Split(options, ",")
	if len(clusterIdWithStateList) == 0 {
		return nil, fmt.Errorf("clusters state change options are insufficient")
	}

	clusterStates := make([]*clusterState, 0)
	for _, clusterIdWithState := range clusterIdWithStateList {
		clusterId := ""
		eqIdx := strings.Index(clusterIdWithState, "=")
		if eqIdx > -1 {
			clusterId = clusterIdWithState[:eqIdx]
			clusterIdWithState = clusterIdWithState[eqIdx+1:]
		}
		state, err := strconv.ParseInt(clusterIdWithState, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("clusters state change options are wrong")
		}
		clusterStates = append(
			clusterStates, &clusterState{
				clusterId: clusterId,
				state:     common.States(state),
			},
		)
	}

	return clusterStates, nil
}

func (m *managerRouter) describeRestoreSnapshotOptions(options string) (string, uint64, error) {
	clusterId := ""
	eqIdx := strings.Index(options, "=")
	if eqIdx > -1 {
		clusterId = options[:eqIdx]
		options = options[eqIdx+1:]
	}
	snapshotIndex, err := strconv.ParseUint(options, 10, 64)
	if err != nil {
		return "", 0, err
	}

	return clusterId, snapshotIndex, nil
}
