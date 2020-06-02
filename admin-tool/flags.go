package main

import (
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"
)

type addNode struct {
	clusterId string
	addresses []string
}

func (a *addNode) String() string {
	if a == nil {
		return ""
	}
	return fmt.Sprintf("%s=%s", a.clusterId, strings.Join(a.addresses, ","))
}

func (a *addNode) Set(value string) error {
	eqIdx := strings.Index(value, "=")
	if eqIdx == -1 {
		return fmt.Errorf("input is not suitable")
	}

	ana := strings.Split(value[eqIdx+1:], ",")
	if len(ana) > 0 && len(ana[0]) == 0 {
		ana = []string{}
	}

	a.clusterId = value[:eqIdx]
	a.addresses = ana

	return nil
}

type flagContainer struct {
	managerAddress     string
	createCluster      []string
	deleteCluster      string
	moveCluster        []string
	balanceClusters    []string
	balanceAllClusters bool
	repairConsistency  string
	addNode            addNode
	removeNode         string
	unfreeze           []string
	unfreezeAll        bool
	createSnapshot     string
	deleteSnapshot     string
	restoreSnapshot    string
	syncClusters       bool
	getCluster         string
	getClusters        bool
	help               bool
	version            bool

	active string
}

func (f *flagContainer) Define(v string) int {
	if f.help {
		fmt.Printf("Kertish-dfs Admin (v%s) usage: \n", v)
		fmt.Println()

		return 1
	}

	if f.version {
		f.active = "version"
		return 0
	}

	activeCount := 0
	if len(f.createCluster) != 0 {
		activeCount++
		f.active = "createCluster"
	}

	if len(f.deleteCluster) != 0 {
		activeCount++
		f.active = "deleteCluster"
	}

	if len(f.moveCluster) != 0 {
		if len(f.moveCluster) != 2 {
			fmt.Println("you should define source and target cluster ids")
			fmt.Println()
			return 1
		}
		activeCount++
		f.active = "moveCluster"
	}

	if len(f.balanceClusters) != 0 || f.balanceAllClusters {
		if len(f.balanceClusters) == 1 {
			fmt.Println("you should define at least two cluster id or leave empty for all")
			fmt.Println()
			return 1
		}
		activeCount++
		f.active = "balanceClusters"
	}

	if len(f.repairConsistency) != 0 {
		activeCount++
		f.active = "repairConsistency"
	}

	if len(f.addNode.clusterId) > 0 && len(f.addNode.addresses) > 0 {
		activeCount++
		f.active = "addNode"
	}

	if len(f.removeNode) != 0 {
		activeCount++
		f.active = "removeNode"
	}

	if len(f.unfreeze) != 0 || f.unfreezeAll {
		activeCount++
		f.active = "unFreeze"
	}

	if len(f.createSnapshot) != 0 {
		activeCount++
		f.active = "createSnapshot"
	}

	if len(f.deleteSnapshot) != 0 {
		paramTest := f.deleteSnapshot

		eqIdx := strings.Index(paramTest, "=")
		if eqIdx == -1 {
			fmt.Println("you should define the snapshot index for the cluster")
			fmt.Println()
			return 1
		}

		clusterId := paramTest[:eqIdx]
		if len(clusterId) == 0 {
			fmt.Println("you should define the target cluster id")
			fmt.Println()
			return 1
		}

		_, err := strconv.ParseUint(paramTest[eqIdx+1:], 10, 64)
		if err != nil {
			fmt.Println("snapshot index should be 0 or positive numeric value")
			fmt.Println()
			return 1
		}

		activeCount++
		f.active = "deleteSnapshot"
	}

	if len(f.restoreSnapshot) != 0 {
		paramTest := f.restoreSnapshot

		eqIdx := strings.Index(paramTest, "=")
		if eqIdx == -1 {
			fmt.Println("you should define the snapshot index for the cluster")
			fmt.Println()
			return 1
		}

		clusterId := paramTest[:eqIdx]
		if len(clusterId) == 0 {
			fmt.Println("you should define the target cluster id")
			fmt.Println()
			return 1
		}

		_, err := strconv.ParseUint(paramTest[eqIdx+1:], 10, 64)
		if err != nil {
			fmt.Println("snapshot index should be 0 or positive numeric value")
			fmt.Println()
			return 1
		}

		activeCount++
		f.active = "restoreSnapshot"
	}

	if f.syncClusters {
		activeCount++
		f.active = "syncClusters"
	}

	if len(f.getCluster) > 0 {
		activeCount++
		f.active = "getCluster"
	}

	if f.getClusters {
		activeCount++
		f.active = "getClusters"
	}

	if activeCount == 0 {
		fmt.Printf("Kertish-dfs Admin (v%s) usage: \n", v)
		fmt.Println()

		return 1
	}

	if activeCount > 1 {
		fmt.Println("accepts only one operation request at a time")

		return 2
	}

	return 0
}

func defineFlags(v string) *flagContainer {
	set := flag.NewFlagSet("dfs", flag.ContinueOnError)

	var managerAddress string
	set.StringVar(&managerAddress, `manager-address`, "localhost:9400", `Points the end point of manager to work with.`)

	var createCluster string
	set.StringVar(&createCluster, `create-cluster`, "", `Creates data nodes cluster. Provide data node binding addresses to create cluster. Node Manager will decide which data node will be master and which others are slave.
Ex: 192.168.0.1:9430,192.168.0.2:9430`)

	var deleteCluster string
	set.StringVar(&deleteCluster, `delete-cluster`, "", `Deletes data nodes cluster. Provide cluster id to delete.`)

	var moveCluster string
	set.StringVar(&moveCluster, `move-cluster`, "", `Moves cluster data between clusters. Provide cluster source and target ids to move cluster.
Ex: sourceClusterId,targetClusterId`)

	var balanceClusters string
	set.StringVar(&balanceClusters, `balance-clusters`, "", `Balance data weight between clusters. Provide at least two cluster ids to balance the data between or leave empty to apply all clusters in the setup.
Ex: clusterId,clusterId`)

	var getCluster string
	set.StringVar(&getCluster, `get-cluster`, "", `Gets and prints cluster information.`)

	set.Bool(`get-clusters`, false, `Gets and prints all clusters information.`)

	addNode := addNode{}
	set.Var(&addNode, `add-node`, `Adds more nodes to the existent cluster. Node Manager will decide for the priority of data nodes.
Ex: clusterId=192.168.0.1:9430,192.168.0.2:9430`)

	var removeNode string
	set.StringVar(&removeNode, `remove-node`, "", `Removes the node from its cluster.`)

	var unFreeze string
	set.StringVar(&unFreeze, `unfreeze`, "", `Unfreeze the frozen clusters to accept data. Provide cluster ids to unfreeze or leave empty to apply all clusters in the setup. Ex: clusterId,clusterId`)

	var repairConsistency string
	set.StringVar(&repairConsistency, `repair-consistency`, "", `Repair file chunk node distribution consistency in metadata and data nodes and mark as zombie for the broken ones. Provide repair model for consistency repairing operation or leave empty to run full repair. Possible repair models (full, structure, integrity)`)

	var createSnapshot string
	set.StringVar(&createSnapshot, `create-snapshot`, "", `Creates snapshot on a cluster. Provide cluster id to create snapshot.`)

	var deleteSnapshot string
	set.StringVar(&deleteSnapshot, `delete-snapshot`, "", `Deletes a snapshot on a cluster. Provide cluster id with snapshot index to be deleted.
Ex: clusterId=snapshotIndex`)

	var restoreSnapshot string
	set.StringVar(&restoreSnapshot, `restore-snapshot`, "", `Restores a snapshot in the cluster. Provide cluster id with snapshot index to be restored.
Ex: clusterId=snapshotIndex`)

	set.Bool(`sync-clusters`, false, `Synchronise all clusters and their nodes for data consistency`)
	set.Bool(`help`, false, `Print this usage documentation`)
	set.Bool(`version`, false, `Print release version`)

	args := os.Args[1:]
	for i, arg := range args {
		idx := strings.Index(arg, "-balance-clusters")
		if idx == -1 {
			continue
		}
		if len(args) > i+1 && !strings.HasPrefix(args[i+1], "-") {
			break
		}
		args = append(append(args[:i+1], "*"), args[i+1:]...)
		break
	}

	for i, arg := range args {
		idx := strings.Index(arg, "-unfreeze")
		if idx == -1 {
			continue
		}
		if len(args) > i+1 && !strings.HasPrefix(args[i+1], "-") {
			break
		}
		args = append(append(args[:i+1], "*"), args[i+1:]...)
		break
	}

	for i, arg := range args {
		idx := strings.Index(arg, "-repair-consistency")
		if idx == -1 {
			continue
		}
		if len(args) > i+1 && !strings.HasPrefix(args[i+1], "-") {
			break
		}
		args = append(append(args[:i+1], "full"), args[i+1:]...)
		break
	}
	set.Parse(args)

	cc := strings.Split(createCluster, ",")
	if len(cc) > 0 && len(cc[0]) == 0 {
		cc = []string{}
	}

	mc := strings.Split(moveCluster, ",")
	if len(mc) != 2 || len(mc) == 2 && len(mc[0]) == 0 && len(mc[1]) == 0 {
		mc = []string{}
	}

	bac := false
	bc := strings.Split(balanceClusters, ",")
	if len(bc) > 0 && len(bc[0]) == 0 || strings.Compare(bc[0], "*") == 0 {
		bac = strings.Compare(bc[0], "*") == 0
		bc = []string{}
	}

	ufa := false
	uf := strings.Split(unFreeze, ",")
	if len(uf) > 0 && len(uf[0]) == 0 || strings.Compare(unFreeze, "*") == 0 {
		ufa = strings.Compare(uf[0], "*") == 0
		uf = []string{}
	}

	fc := &flagContainer{
		managerAddress:     managerAddress,
		createCluster:      cc,
		deleteCluster:      deleteCluster,
		moveCluster:        mc,
		balanceClusters:    bc,
		balanceAllClusters: bac,
		repairConsistency:  repairConsistency,
		addNode:            addNode,
		removeNode:         removeNode,
		unfreeze:           uf,
		unfreezeAll:        ufa,
		createSnapshot:     createSnapshot,
		deleteSnapshot:     deleteSnapshot,
		restoreSnapshot:    restoreSnapshot,
		syncClusters:       strings.Index(strings.Join(os.Args, " "), "sync-clusters") > -1,
		getCluster:         getCluster,
		getClusters:        strings.Index(strings.Join(os.Args, " "), "get-clusters") > -1,
		help:               strings.Index(strings.Join(os.Args, " "), "-help") > -1,
		version:            strings.Index(strings.Join(os.Args, " "), "-version") > -1,
	}

	switch fc.Define(v) {
	case 1:
		set.PrintDefaults()
		os.Exit(0)
	case 2:
		os.Exit(2)
	}

	return fc
}
