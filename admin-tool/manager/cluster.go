package manager

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/freakmaxi/kertish-dfs/basics/common"
)

const managerEndPoint = "/client/manager"

var client = http.Client{Timeout: time.Hour * 24 * 7} // one week timeout

func CreateCluster(managerAddr []string, addresses []string) error {
	req, err := http.NewRequest(http.MethodPost, fmt.Sprintf("http://%s%s", managerAddr[0], managerEndPoint), nil)
	if err != nil {
		return err
	}
	req.Header.Set("X-Action", "register")
	req.Header.Set("X-Options", strings.Join(addresses, ","))

	res, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("%s: manager node is not reachable", managerAddr[0])
	}
	defer func() { _ = res.Body.Close() }()

	if res.StatusCode != 200 {
		var e common.Error
		if err := json.NewDecoder(res.Body).Decode(&e); err != nil {
			if _, ok := err.(*json.SyntaxError); ok {
				return fmt.Errorf("dfs manager returned with an unrecognisable status code: %d", res.StatusCode)
			}
			return err
		}
		return fmt.Errorf(e.Message)
	}

	var c common.Cluster
	if err := json.NewDecoder(res.Body).Decode(&c); err != nil {
		return err
	}
	fmt.Printf("Cluster is created as offline: %s\n", c.Id)
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
	req, err := http.NewRequest(http.MethodDelete, fmt.Sprintf("http://%s%s", managerAddr[0], managerEndPoint), nil)
	if err != nil {
		return err
	}
	req.Header.Set("X-Action", "unregister")
	req.Header.Set("X-Options", fmt.Sprintf("c,%s", clusterId))

	res, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("%s: manager node is not reachable", managerAddr[0])
	}
	defer func() { _ = res.Body.Close() }()

	if res.StatusCode != 200 {
		var e common.Error
		if err := json.NewDecoder(res.Body).Decode(&e); err != nil {
			if _, ok := err.(*json.SyntaxError); ok {
				return fmt.Errorf("dfs manager returned with an unrecognisable status code: %d", res.StatusCode)
			}
			return err
		}
		return fmt.Errorf(e.Message)
	}

	fmt.Printf("Cluster is deleted: %s\n", clusterId)

	return nil
}

func MoveCluster(managerAddr []string, clusterIds []string) error {
	req, err := http.NewRequest(http.MethodGet, fmt.Sprintf("http://%s%s", managerAddr[0], managerEndPoint), nil)
	if err != nil {
		return err
	}
	req.Header.Set("X-Action", "move")
	req.Header.Set("X-Options", strings.Join(clusterIds, ","))

	res, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("%s: manager node is not reachable", managerAddr[0])
	}
	defer func() { _ = res.Body.Close() }()

	if res.StatusCode != 200 {
		if res.StatusCode == 422 {
			return fmt.Errorf("")
		}

		var e common.Error
		if err := json.NewDecoder(res.Body).Decode(&e); err != nil {
			if _, ok := err.(*json.SyntaxError); ok {
				return fmt.Errorf("dfs manager returned with an unrecognisable status code: %d", res.StatusCode)
			}
			return err
		}
		return fmt.Errorf(e.Message)
	}

	return nil
}

func BalanceClusters(managerAddr []string, clusterIds []string) error {
	req, err := http.NewRequest(http.MethodGet, fmt.Sprintf("http://%s%s", managerAddr[0], managerEndPoint), nil)
	if err != nil {
		return err
	}
	req.Header.Set("X-Action", "balance")
	req.Header.Set("X-Options", strings.Join(clusterIds, ","))

	res, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("%s: manager node is not reachable", managerAddr[0])
	}
	defer func() { _ = res.Body.Close() }()

	if res.StatusCode != 200 {
		if res.StatusCode == 422 {
			return fmt.Errorf("")
		}

		var e common.Error
		if err := json.NewDecoder(res.Body).Decode(&e); err != nil {
			if _, ok := err.(*json.SyntaxError); ok {
				return fmt.Errorf("dfs manager returned with an unrecognisable status code: %d", res.StatusCode)
			}
			return err
		}
		return fmt.Errorf(e.Message)
	}

	return nil
}

func AddNode(managerAddr []string, clusterId string, addresses []string) error {
	req, err := http.NewRequest(http.MethodPost, fmt.Sprintf("http://%s%s", managerAddr[0], managerEndPoint), nil)
	if err != nil {
		return err
	}
	req.Header.Set("X-Action", "register")
	req.Header.Set("X-Options", fmt.Sprintf("%s=%s", clusterId, strings.Join(addresses, ",")))

	res, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("%s: manager node is not reachable", managerAddr[0])
	}
	defer func() { _ = res.Body.Close() }()

	if res.StatusCode != 200 {
		var e common.Error
		if err := json.NewDecoder(res.Body).Decode(&e); err != nil {
			if _, ok := err.(*json.SyntaxError); ok {
				return fmt.Errorf("dfs manager returned with an unrecognisable status code: %d", res.StatusCode)
			}
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
	req, err := http.NewRequest(http.MethodDelete, fmt.Sprintf("http://%s%s", managerAddr[0], managerEndPoint), nil)
	if err != nil {
		return err
	}
	req.Header.Set("X-Action", "unregister")
	req.Header.Set("X-Options", fmt.Sprintf("n,%s", nodeId))

	res, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("%s: manager node is not reachable", managerAddr[0])
	}
	defer func() { _ = res.Body.Close() }()

	if res.StatusCode != 200 {
		var e common.Error
		if err := json.NewDecoder(res.Body).Decode(&e); err != nil {
			if _, ok := err.(*json.SyntaxError); ok {
				return fmt.Errorf("dfs manager returned with an unrecognisable status code: %d", res.StatusCode)
			}
			return err
		}
		return fmt.Errorf(e.Message)
	}

	fmt.Printf("Node is removed from cluster: %s\n", nodeId)

	return nil
}

func ChangeState(managerAddr []string, clusterIds []string, state common.States) error {
	req, err := http.NewRequest(http.MethodPut, fmt.Sprintf("http://%s%s", managerAddr[0], managerEndPoint), nil)
	if err != nil {
		return err
	}

	options := make([]string, 0)
	for _, clusterId := range clusterIds {
		options = append(options, fmt.Sprintf("%s=%d", clusterId, state))
	}
	if len(options) == 0 {
		options = append(options, fmt.Sprintf("=%d", state))
	}
	req.Header.Set("X-Action", "state")
	req.Header.Set("X-Options", strings.Join(options, ","))

	res, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("%s: manager node is not reachable", managerAddr[0])
	}
	defer func() { _ = res.Body.Close() }()

	if res.StatusCode != 200 {
		var e common.Error
		if err := json.NewDecoder(res.Body).Decode(&e); err != nil {
			if errors.Is(err, io.EOF) {
				return fmt.Errorf("")
			}
			if _, ok := err.(*json.SyntaxError); ok {
				return fmt.Errorf("dfs manager returned with an unrecognisable status code: %d", res.StatusCode)
			}
			return err
		}
		return fmt.Errorf(e.Message)
	}

	stateString := "Online"
	switch state {
	case common.StateReadonly:
		stateString = "Online (RO)"
	case common.StateOffline:
		stateString = "Offline"
	}

	fmt.Println()
	fmt.Printf("Clusters state has been changed to %s...\n", stateString)

	return nil
}

func CreateSnapshot(managerAddr []string, clusterId string) error {
	req, err := http.NewRequest(http.MethodPost, fmt.Sprintf("http://%s%s", managerAddr[0], managerEndPoint), nil)
	if err != nil {
		return err
	}
	req.Header.Set("X-Action", "snapshot")
	req.Header.Set("X-Options", clusterId)

	res, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("%s: manager node is not reachable", managerAddr[0])
	}
	defer func() { _ = res.Body.Close() }()

	if res.StatusCode != 200 {
		if res.StatusCode == 422 {
			return fmt.Errorf("")
		}

		var e common.Error
		if err := json.NewDecoder(res.Body).Decode(&e); err != nil {
			if _, ok := err.(*json.SyntaxError); ok {
				return fmt.Errorf("dfs manager returned with an unrecognisable status code: %d", res.StatusCode)
			}
			return err
		}
		return fmt.Errorf(e.Message)
	}

	return nil
}

func DeleteSnapshot(managerAddr []string, clusterId string, snapshotIndex uint64) error {
	req, err := http.NewRequest(http.MethodDelete, fmt.Sprintf("http://%s%s", managerAddr[0], managerEndPoint), nil)
	if err != nil {
		return err
	}
	req.Header.Set("X-Action", "snapshot")
	req.Header.Set("X-Options", fmt.Sprintf("%s=%d", clusterId, snapshotIndex))

	res, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("%s: manager node is not reachable", managerAddr[0])
	}
	defer func() { _ = res.Body.Close() }()

	if res.StatusCode != 200 {
		if res.StatusCode == 422 {
			return fmt.Errorf("")
		}

		var e common.Error
		if err := json.NewDecoder(res.Body).Decode(&e); err != nil {
			if _, ok := err.(*json.SyntaxError); ok {
				return fmt.Errorf("dfs manager returned with an unrecognisable status code: %d", res.StatusCode)
			}
			return err
		}
		return fmt.Errorf(e.Message)
	}

	return nil
}

func RestoreSnapshot(managerAddr []string, clusterId string, snapshotIndex uint64) error {
	req, err := http.NewRequest(http.MethodPut, fmt.Sprintf("http://%s%s", managerAddr[0], managerEndPoint), nil)
	if err != nil {
		return err
	}
	req.Header.Set("X-Action", "snapshot")
	req.Header.Set("X-Options", fmt.Sprintf("%s=%d", clusterId, snapshotIndex))

	res, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("%s: manager node is not reachable", managerAddr[0])
	}
	defer func() { _ = res.Body.Close() }()

	if res.StatusCode != 200 {
		if res.StatusCode == 422 {
			return fmt.Errorf("")
		}

		var e common.Error
		if err := json.NewDecoder(res.Body).Decode(&e); err != nil {
			if _, ok := err.(*json.SyntaxError); ok {
				return fmt.Errorf("dfs manager returned with an unrecognisable status code: %d", res.StatusCode)
			}
			return err
		}
		return fmt.Errorf(e.Message)
	}

	return nil
}

func SyncClusters(managerAddr []string, clusterId string) error {
	req, err := http.NewRequest(http.MethodGet, fmt.Sprintf("http://%s%s", managerAddr[0], managerEndPoint), nil)
	if err != nil {
		return err
	}
	req.Header.Set("X-Action", "sync")
	req.Header.Set("X-Options", clusterId)

	res, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("%s: manager node is not reachable", managerAddr[0])
	}
	defer func() { _ = res.Body.Close() }()

	if res.StatusCode != 202 {
		var e common.Error
		if err := json.NewDecoder(res.Body).Decode(&e); err != nil {
			if _, ok := err.(*json.SyntaxError); ok {
				return fmt.Errorf("dfs manager returned with an unrecognisable status code: %d", res.StatusCode)
			}
			return err
		}
		return fmt.Errorf(e.Message)
	}

	return nil
}

func RepairConsistency(managerAddr []string, repairModel string) error {
	req, err := http.NewRequest(http.MethodGet, fmt.Sprintf("http://%s%s", managerAddr[0], managerEndPoint), nil)
	if err != nil {
		return err
	}
	req.Header.Set("X-Action", "repair")
	req.Header.Set("X-Options", repairModel)

	res, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("%s: manager node is not reachable", managerAddr[0])
	}
	defer func() { _ = res.Body.Close() }()

	if res.StatusCode != 202 {
		var e common.Error
		if err := json.NewDecoder(res.Body).Decode(&e); err != nil {
			if _, ok := err.(*json.SyntaxError); ok {
				return fmt.Errorf("dfs manager returned with an unrecognisable status code: %d", res.StatusCode)
			}
			return err
		}
		return fmt.Errorf(e.Message)
	}

	return nil
}

func GetClusters(managerAddr []string, clusterId string) error {
	req, err := http.NewRequest(http.MethodGet, fmt.Sprintf("http://%s%s", managerAddr[0], managerEndPoint), nil)
	if err != nil {
		return err
	}
	req.Header.Set("X-Action", "clusters")
	req.Header.Set("X-Options", clusterId)

	res, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("%s: manager node is not reachable", managerAddr[0])
	}
	defer func() { _ = res.Body.Close() }()

	if res.StatusCode != 200 {
		var e common.Error
		if err := json.NewDecoder(res.Body).Decode(&e); err != nil {
			if _, ok := err.(*json.SyntaxError); ok {
				return fmt.Errorf("dfs manager returned with an unrecognisable status code: %d", res.StatusCode)
			}
			return err
		}
		return fmt.Errorf(e.Message)
	}

	var c []common.Cluster
	if err := json.NewDecoder(res.Body).Decode(&c); err != nil {
		return err
	}

	total := uint64(0)
	used := uint64(0)
	for _, cluster := range c {
		total += cluster.Size
		used += cluster.Used

		fmt.Printf("Cluster Details: %s\n", cluster.Id)
		for _, n := range cluster.Nodes {
			mode := "(SLAVE) "
			if n.Master {
				mode = "(MASTER)"
			}
			fmt.Printf("      Data Node: %s %s -> %s\n", n.Address, mode, n.Id)
		}
		fmt.Printf("      Size:      %d (%d Gb)\n", cluster.Size, cluster.Size/(1024*1024*1024))
		fmt.Printf("      Available: %d (%d Gb)\n", cluster.Available(), cluster.Available()/(1024*1024*1024))
		fmt.Printf("      Weight:    %.2f\n", cluster.Weight())
		fmt.Printf("      Status:    %s\n", cluster.StateString())
		if cluster.Maintain {
			fmt.Printf("      Maintain:  InProgress (%s)\n", cluster.MaintainTopic)
		}
		if len(cluster.Snapshots) > 0 {
			for i, snapshot := range cluster.Snapshots {
				if i == 0 {
					fmt.Printf("      Snapshots: %-4d %s\n", i, snapshot.Format(common.FriendlyTimeFormatWithSeconds))
					continue
				}
				fmt.Printf("                 %-4d %s\n", i, snapshot.Format(common.FriendlyTimeFormatWithSeconds))
			}
		}
		fmt.Println()
	}

	if len(clusterId) == 0 {
		fmt.Println("Setup Summary:")
		fmt.Printf("      Total Size:      %d (%d Gb)\n", total, total/(1024*1024*1024))
		available := total - used
		fmt.Printf("      Total Available: %d (%d Gb)\n", available, available/(1024*1024*1024))
		if strings.Compare(res.Header.Get("X-Repairing"), "true") == 0 {
			fmt.Printf("      Repairing:       In progress\n")
		} else {
			repairCompletedTimestamp := res.Header.Get("X-Repairing-Timestamp")
			if len(repairCompletedTimestamp) > 0 {
				repairCompletedTime, err := time.Parse(time.RFC3339, repairCompletedTimestamp)
				if err == nil {
					fmt.Printf("      Repairing:       Completed at %s\n", repairCompletedTime.Local().Format(common.FriendlyTimeFormatWithSeconds))
				}
			}
		}
		fmt.Println()
	}

	return nil
}

func GetReport(managerAddr []string) error {
	req, err := http.NewRequest(http.MethodGet, fmt.Sprintf("http://%s%s", managerAddr[0], managerEndPoint), nil)
	if err != nil {
		return err
	}
	req.Header.Set("X-Action", "health")

	res, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("%s: manager node is not reachable", managerAddr[0])
	}
	defer func() { _ = res.Body.Close() }()

	if res.StatusCode != 200 {
		var e common.Error
		if err := json.NewDecoder(res.Body).Decode(&e); err != nil {
			if _, ok := err.(*json.SyntaxError); ok {
				return fmt.Errorf("dfs manager returned with an unrecognisable status code: %d", res.StatusCode)
			}
			return err
		}
		return fmt.Errorf(e.Message)
	}

	var r map[string]common.NodeList
	if err := json.NewDecoder(res.Body).Decode(&r); err != nil {
		return err
	}

	for clusterId, nodeList := range r {
		fmt.Printf("Cluster Details: %s\n", clusterId)
		for i, n := range nodeList {
			mode := "(SLAVE) "
			if n.Master {
				mode = "(MASTER)"
			}

			status := "Online"
			switch n.Quality {
			case -2:
				status = "Dns Error"
			case -1:
				status = "Paralysed"
			default:
				status = fmt.Sprintf("%s (%d ms)", status, n.Quality)
			}

			if i == 0 {
				fmt.Printf("      Data Node: %s %s %s -> %s\n", n.Id, n.Address, mode, status)
				continue
			}
			fmt.Printf("                 %s %s %s -> %s\n", n.Id, n.Address, mode, status)
		}
		fmt.Println()
	}

	return nil
}
