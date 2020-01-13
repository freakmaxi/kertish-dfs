package main

import (
	"flag"
	"fmt"
	"os"
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
	managerAddress string
	createCluster  []string
	deleteCluster  string
	addNode        addNode
	removeNode     string
	syncClusters   bool
	getCluster     string
	getClusters    bool

	active string
}

func (f *flagContainer) Define() int {
	activeCount := 0
	if len(f.createCluster) != 0 {
		activeCount++
		f.active = "createCluster"
	}

	if len(f.deleteCluster) != 0 {
		activeCount++
		f.active = "deleteCluster"
	}

	if len(f.addNode.clusterId) > 0 && len(f.addNode.addresses) > 0 {
		activeCount++
		f.active = "addNode"
	}

	if len(f.removeNode) != 0 {
		activeCount++
		f.active = "removeNode"
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
		fmt.Println("2020-dfs Manager Admin usage: ")
		fmt.Println()

		return 1
	}

	if activeCount > 1 {
		fmt.Println("accepts only one operation request at a time")

		return 2
	}

	return 0
}

func defineFlags() *flagContainer {
	set := flag.NewFlagSet("dfs", flag.ContinueOnError)

	var managerAddress string
	set.StringVar(&managerAddress, `manager-address`, "localhost:9400", `Points the end point of manager to work with.`)

	var createCluster string
	set.StringVar(&createCluster, `create-cluster`, "", `Creates data nodes cluster. Provide data node binding addresses to create cluster. Node Manager will decide which data node will be master and which others are slave.
Ex: 192.168.0.1:9430,192.168.0.2:9430`)

	var deleteCluster string
	set.StringVar(&deleteCluster, `delete-cluster`, "", `Deletes data nodes cluster. Provide cluster id to delete.`)

	var getCluster string
	set.StringVar(&getCluster, `get-cluster`, "", `Gets and prints cluster information.`)

	set.Bool(`get-clusters`, false, `Gets and prints all clusters information.`)

	addNode := addNode{}
	set.Var(&addNode, `add-node`, `Adds more nodes to the existent cluster. Node Manager will decide for the priority of data nodes.
Ex: clusterId=192.168.0.1:9430,192.168.0.2:9430`)

	var removeNode string
	set.StringVar(&removeNode, `remove-node`, "", `Removes the node from its cluster.`)

	set.Bool(`sync-clusters`, false, `Synchronise all clusters and their nodes for data consistency`)

	set.Parse(os.Args[1:])

	cc := strings.Split(createCluster, ",")
	if len(cc) > 0 && len(cc[0]) == 0 {
		cc = []string{}
	}

	fc := &flagContainer{
		managerAddress: managerAddress,
		createCluster:  cc,
		deleteCluster:  deleteCluster,
		addNode:        addNode,
		removeNode:     removeNode,
		syncClusters:   strings.Index(strings.Join(os.Args, " "), "sync-clusters") > -1,
		getCluster:     getCluster,
		getClusters:    strings.Index(strings.Join(os.Args, " "), "get-clusters") > -1,
	}

	switch fc.Define() {
	case 1:
		set.PrintDefaults()
		os.Exit(0)
	case 2:
		os.Exit(2)
	}

	return fc
}
