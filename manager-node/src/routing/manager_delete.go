package routing

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"strings"

	"github.com/freakmaxi/kertish-dfs/basics/src/common"
	"github.com/freakmaxi/kertish-dfs/basics/src/errors"
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
			}
			e := common.NewError(300, err.Error())
			if err := json.NewEncoder(w).Encode(e); err != nil {
				fmt.Printf("ERROR: Delete request is failed. %s\n", err.Error())
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
			}
			e := common.NewError(350, err.Error())
			if err := json.NewEncoder(w).Encode(e); err != nil {
				fmt.Printf("ERROR: Delete request is failed. %s\n", err.Error())
			}
			return
		}
	}

	w.WriteHeader(200)
}

func (m *managerRouter) handleCommit(w http.ResponseWriter, r *http.Request) {
	reservationId := r.Header.Get("X-Reservation-Id")
	clusterMap, err := m.describeReservationCommitOptions(r.Header.Get("X-Options"))

	if len(reservationId) == 0 || err != nil {
		w.WriteHeader(422)
		return
	}

	if err := m.manager.Commit(reservationId, clusterMap); err != nil {
		w.WriteHeader(400)
		e := common.NewError(360, err.Error())
		if err := json.NewEncoder(w).Encode(e); err != nil {
			fmt.Printf("ERROR: Delete request is failed. %s\n", err.Error())
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
		w.WriteHeader(400)
		e := common.NewError(370, err.Error())
		if err := json.NewEncoder(w).Encode(e); err != nil {
			fmt.Printf("ERROR: Delete request is failed. %s\n", err.Error())
		}
		return
	}

	w.WriteHeader(200)
}

func (m *managerRouter) validateDeleteAction(action string) bool {
	switch action {
	case "unregister", "commit", "discard":
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
