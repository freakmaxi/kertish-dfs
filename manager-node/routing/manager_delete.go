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

func (m *managerRouter) handleDelete(w http.ResponseWriter, r *http.Request) {
	action := r.Header.Get("X-Action")

	if !m.validateDeleteAction(action) {
		w.WriteHeader(422)
		return
	}

	switch action {
	case "unregister":
		m.handleUnRegister(w, r)
	case "unfreeze":
		m.handleUnFreeze(w, r)
	case "snapshot":
		m.handleDeleteSnapshot(w, r)
	case "commit":
		m.handleCommit(w, r)
	case "discard":
		m.handleDiscard(w, r)
	default:
		w.WriteHeader(406)
	}
}

func (m *managerRouter) handleUnRegister(w http.ResponseWriter, r *http.Request) {
	idType, id, err := m.describeUnRegisterOptions(r.Header.Get("X-Options"))
	if err != nil {
		w.WriteHeader(422)
		return
	}

	switch idType {
	case "c":
		if err := m.manager.UnRegisterCluster(id); err != nil {
			if err == errors.ErrNotFound {
				w.WriteHeader(404)
			} else {
				w.WriteHeader(500)
				m.logger.Error("Unregister cluster request is failed", zap.Error(err))
			}

			e := common.NewError(300, err.Error())
			if err := json.NewEncoder(w).Encode(e); err != nil {
				m.logger.Error("Response of unregister cluster request is failed", zap.Error(err))
			}
			return
		}
	case "n":
		if err := m.manager.UnRegisterNode(id); err != nil {
			if err == errors.ErrNotFound {
				w.WriteHeader(404)
			} else if err == errors.ErrLastNode {
				w.WriteHeader(423)
			} else {
				w.WriteHeader(500)
				m.logger.Error("Unregister node request is failed", zap.Error(err))
			}

			e := common.NewError(350, err.Error())
			if err := json.NewEncoder(w).Encode(e); err != nil {
				m.logger.Error("Response of unregister node request is failed", zap.Error(err))
			}
			return
		}
	}

	w.WriteHeader(200)
}

func (m *managerRouter) handleUnFreeze(w http.ResponseWriter, r *http.Request) {
	clusterIds := m.describeUnfreezeOptions(r.Header.Get("X-Options"))

	if err := m.manager.UnFreezeClusters(clusterIds); err != nil {
		w.WriteHeader(500)
		m.logger.Error("Unfreeze request is failed", zap.Error(err))

		e := common.NewError(355, err.Error())
		if err := json.NewEncoder(w).Encode(e); err != nil {
			m.logger.Error("Response of unfreeze request is failed", zap.Error(err))
		}
		return
	}

	w.WriteHeader(200)
}

func (m *managerRouter) handleDeleteSnapshot(w http.ResponseWriter, r *http.Request) {
	clusterId, snapshotIndex, err := m.describeDeleteSnapshotOptions(r.Header.Get("X-Options"))
	if len(clusterId) == 0 || err != nil {
		w.WriteHeader(422)
		if err != nil {
			e := common.NewError(380, err.Error())
			if err := json.NewEncoder(w).Encode(e); err != nil {
				m.logger.Error("Response of delete snapshot request is failed", zap.Error(err))
			}
		}
	}

	err = m.manager.DeleteSnapshot(clusterId, snapshotIndex)
	if err == nil {
		return
	}

	w.WriteHeader(500)
	m.logger.Error("Delete snapshot request is failed", zap.Error(err))

	e := common.NewError(380, err.Error())
	if err := json.NewEncoder(w).Encode(e); err != nil {
		m.logger.Error("Response of delete snapshot request is failed", zap.Error(err))
	}
}

func (m *managerRouter) handleCommit(w http.ResponseWriter, r *http.Request) {
	reservationId := r.Header.Get("X-Reservation-Id")
	clusterMap, err := m.describeReservationCommitOptions(r.Header.Get("X-Options"))

	if len(reservationId) == 0 || err != nil {
		w.WriteHeader(422)
		return
	}

	if err := m.manager.Commit(reservationId, clusterMap); err != nil {
		w.WriteHeader(500)
		m.logger.Error("Commit request is failed", zap.Error(err))

		e := common.NewError(360, err.Error())
		if err := json.NewEncoder(w).Encode(e); err != nil {
			m.logger.Error("Response of commit request is failed", zap.Error(err))
		}
		return
	}

	w.WriteHeader(200)
}

func (m *managerRouter) handleDiscard(w http.ResponseWriter, r *http.Request) {
	reservationId := r.Header.Get("X-Reservation-Id")
	if len(reservationId) == 0 {
		w.WriteHeader(422)
		return
	}

	if err := m.manager.Discard(reservationId); err != nil {
		w.WriteHeader(500)
		m.logger.Error("Discard request is failed", zap.Error(err))

		e := common.NewError(370, err.Error())
		if err := json.NewEncoder(w).Encode(e); err != nil {
			m.logger.Error("Response of discard request is failed", zap.Error(err))
		}
		return
	}

	w.WriteHeader(200)
}

func (m *managerRouter) validateDeleteAction(action string) bool {
	switch action {
	case "unregister", "unfreeze", "snapshot", "commit", "discard":
		return true
	}
	return false
}

func (m *managerRouter) describeUnRegisterOptions(options string) (string, string, error) {
	commaIdx := strings.Index(options, ",")
	if commaIdx == -1 || commaIdx > 1 {
		return "", "", os.ErrInvalid
	}

	idType := options[:1]
	if strings.Compare(idType, "c") != 0 && strings.Compare(idType, "n") != 0 {
		return "", "", os.ErrInvalid
	}
	id := options[2:]

	return idType, id, nil
}

func (m *managerRouter) describeDeleteSnapshotOptions(options string) (string, uint64, error) {
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

func (m *managerRouter) describeReservationCommitOptions(options string) (map[string]uint64, error) {
	commitMap := make(map[string]uint64)

	if len(options) == 0 {
		return commitMap, nil
	}

	maps := strings.Split(options, ",")
	for _, m := range maps {
		equalIdx := strings.Index(m, "=")
		if equalIdx < 1 {
			return nil, os.ErrInvalid
		}

		clusterId := m[:equalIdx]
		sizeString := m[equalIdx+1:]

		size, err := strconv.ParseUint(sizeString, 10, 64)
		if err != nil {
			return nil, os.ErrInvalid
		}

		if _, has := commitMap[clusterId]; !has {
			commitMap[clusterId] = 0
		}
		commitMap[clusterId] += size
	}

	return commitMap, nil
}

func (m *managerRouter) describeUnfreezeOptions(options string) []string {
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

	return clusterIds
}
