package routing

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"regexp"
	"strconv"
	"strings"

	"github.com/freakmaxi/2020-dfs/manager-node/src/common"
	"github.com/freakmaxi/2020-dfs/manager-node/src/errors"
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
	case "reserve":
		m.handleReserve(w, r)
	case "readMap", "deleteMap":
		m.handleMap(w, r, strings.Compare(action, "deleteMap") == 0)
	default:
		w.WriteHeader(406)
	}
}

func (m *managerRouter) handleRegister(w http.ResponseWriter, r *http.Request) {
	clusterId, addresses, err := m.describeRegisterOptions(r.Header.Get("X-Options"))
	if err != nil {
		w.WriteHeader(422)
		return
	}

	var cluster *common.Cluster
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
			fmt.Printf("ERROR: Post request is failed. %s\n", err.Error())
		}
		return
	}

	if err == errors.ErrRegistered {
		w.WriteHeader(409)
	} else {
		w.WriteHeader(400)
	}
	e := common.NewError(200, err.Error())
	if err := json.NewEncoder(w).Encode(e); err != nil {
		fmt.Printf("ERROR: Post request is failed. %s\n", err.Error())
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
			fmt.Printf("ERROR: Post request is failed. %s\n", err.Error())
		}
		return
	}

	if err == errors.ErrNoDiskSpace {
		w.WriteHeader(507)
	} else {
		w.WriteHeader(400)
	}
	e := common.NewError(210, err.Error())
	if err := json.NewEncoder(w).Encode(e); err != nil {
		fmt.Printf("ERROR: Post request is failed. %s\n", err.Error())
	}
}

func (m *managerRouter) handleMap(w http.ResponseWriter, r *http.Request, deleteMap bool) {
	sha512HexList := strings.Split(r.Header.Get("X-Options"), ",")
	if len(sha512HexList) == 0 {
		w.WriteHeader(422)
		return
	}

	clusterMapping, err := m.manager.Map(sha512HexList, deleteMap)
	if err == nil {
		if err := json.NewEncoder(w).Encode(clusterMapping); err != nil {
			fmt.Printf("ERROR: Post request is failed. %s\n", err.Error())
		}
		return
	}

	if err == errors.ErrNoAvailableNode {
		w.WriteHeader(503)
	} else {
		w.WriteHeader(400)
	}
	e := common.NewError(220, err.Error())
	if err := json.NewEncoder(w).Encode(e); err != nil {
		fmt.Printf("ERROR: Post request is failed. %s\n", err.Error())
	}
}

func (m *managerRouter) validatePostAction(action string) bool {
	switch action {
	case "register", "reserve", "readMap", "deleteMap":
		return true
	}
	return false
}

func (m *managerRouter) describeRegisterOptions(options string) (string, []string, error) {
	clusterId := ""
	eqIdx := strings.Index(options, "=")
	if eqIdx > -1 {
		clusterId = options[:eqIdx]
		options = options[eqIdx+1:]
	}

	ipAddressMask, _ := regexp.Compile(`\d{1,3}.\d{1,3}.\d{1,3}.\d{1,3}(:\d{1,5})?`)
	addresses := strings.Split(options, ",")

	for _, address := range addresses {
		if !ipAddressMask.MatchString(address) {
			return "", nil, os.ErrInvalid
		}
	}
	return clusterId, addresses, nil
}
