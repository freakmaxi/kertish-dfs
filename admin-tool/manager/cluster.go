package manager

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/freakmaxi/2020-dfs/admin-tool/common"
)

const managerEndPoint = "/client/manager"

var client = http.Client{Timeout: time.Hour * 24 * 7} // one week timeout

func CreateCluster(managerAddr []string, addresses []string) error {
	req, err := http.NewRequest("POST", fmt.Sprintf("http://%s%s", managerAddr[0], managerEndPoint), nil)
	if err != nil {
		return err
	}
	req.Header.Set("X-Action", "register")
	req.Header.Set("X-Options", strings.Join(addresses, ","))

	res, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("%s: manager node is not reachable", managerAddr[0])
	}

	if res.StatusCode != 200 {
		var e common.Error
		if err := json.NewDecoder(res.Body).Decode(&e); err != nil {
			return err
		}
		return fmt.Errorf(e.Message)
	}

	var c common.Cluster
	if err := json.NewDecoder(res.Body).Decode(&c); err != nil {
		return err
	}
	fmt.Printf("Cluster is created: %s\n", c.Id)
	for _, n := range c.Nodes {
		mode := "SLAVE"
		if n.Master {
			mode = "MASTER"
		}
		fmt.Printf("         Data Node: %s (%s) -> %s\n", n.Address, mode, n.Id)
	}

	return nil
}

func DeleteCluster(managerAddr []string, clusterId string) error {
	req, err := http.NewRequest("DELETE", fmt.Sprintf("http://%s%s", managerAddr[0], managerEndPoint), nil)
	if err != nil {
		return err
	}
	req.Header.Set("X-Action", "unregister")
	req.Header.Set("X-Options", fmt.Sprintf("c,%s", clusterId))

	res, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("%s: manager node is not reachable", managerAddr[0])
	}

	if res.StatusCode != 200 {
		var e common.Error
		if err := json.NewDecoder(res.Body).Decode(&e); err != nil {
			return err
		}
		return fmt.Errorf(e.Message)
	}

	fmt.Printf("Cluster is deleted: %s\n", clusterId)

	return nil
}

func AddNode(managerAddr []string, clusterId string, addresses []string) error {
	req, err := http.NewRequest("POST", fmt.Sprintf("http://%s%s", managerAddr[0], managerEndPoint), nil)
	if err != nil {
		return err
	}
	req.Header.Set("X-Action", "register")
	req.Header.Set("X-Options", fmt.Sprintf("%s=%s", clusterId, strings.Join(addresses, ",")))

	res, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("%s: manager node is not reachable", managerAddr[0])
	}

	if res.StatusCode != 200 {
		var e common.Error
		if err := json.NewDecoder(res.Body).Decode(&e); err != nil {
			return err
		}
		return fmt.Errorf(e.Message)
	}

	var c common.Cluster
	if err := json.NewDecoder(res.Body).Decode(&c); err != nil {
		return err
	}
	fmt.Printf("Node is added to cluster: %s\n", c.Id)
	for _, n := range c.Nodes {
		mode := "SLAVE"
		if n.Master {
			mode = "MASTER"
		}
		fmt.Printf("			 Data Node: %s (%s) -> %s\n", n.Address, mode, n.Id)
	}

	return nil
}

func RemoveNode(managerAddr []string, nodeId string) error {
	req, err := http.NewRequest("DELETE", fmt.Sprintf("http://%s%s", managerAddr[0], managerEndPoint), nil)
	if err != nil {
		return err
	}
	req.Header.Set("X-Action", "unregister")
	req.Header.Set("X-Options", fmt.Sprintf("n,%s", nodeId))

	res, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("%s: manager node is not reachable", managerAddr[0])
	}

	if res.StatusCode != 200 {
		var e common.Error
		if err := json.NewDecoder(res.Body).Decode(&e); err != nil {
			return err
		}
		return fmt.Errorf(e.Message)
	}

	fmt.Printf("Node is removed from cluster: %s\n", nodeId)

	return nil
}

func SyncClusters(managerAddr []string) error {
	req, err := http.NewRequest("GET", fmt.Sprintf("http://%s%s", managerAddr[0], managerEndPoint), nil)
	if err != nil {
		return err
	}
	req.Header.Set("X-Action", "sync")

	res, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("%s: manager node is not reachable", managerAddr[0])
	}

	if res.StatusCode != 200 {
		var e common.Error
		if err := json.NewDecoder(res.Body).Decode(&e); err != nil {
			return err
		}
		return fmt.Errorf(e.Message)
	}

	return nil
}

func GetClusters(managerAddr []string, clusterId string) error {
	req, err := http.NewRequest("GET", fmt.Sprintf("http://%s%s", managerAddr[0], managerEndPoint), nil)
	if err != nil {
		return err
	}
	req.Header.Set("X-Action", "clusters")
	req.Header.Set("X-Options", clusterId)

	res, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("%s: manager node is not reachable", managerAddr[0])
	}

	if res.StatusCode != 200 {
		var e common.Error
		if err := json.NewDecoder(res.Body).Decode(&e); err != nil {
			return err
		}
		return fmt.Errorf(e.Message)
	}

	var c []common.Cluster
	if err := json.NewDecoder(res.Body).Decode(&c); err != nil {
		return err
	}
	for _, cluster := range c {
		fmt.Printf("Cluster Details: %s\n", cluster.Id)
		for _, n := range cluster.Nodes {
			mode := "SLAVE"
			if n.Master {
				mode = "MASTER"
			}
			fmt.Printf("      Data Node: %s (%s) -> %s\n", n.Address, mode, n.Id)
		}
		fmt.Println()
	}

	return nil
}
