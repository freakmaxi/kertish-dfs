package main

import (
	"bufio"
	"fmt"
	"os"

	"github.com/freakmaxi/kertish-dfs/admin-tool/common"
	"github.com/freakmaxi/kertish-dfs/admin-tool/manager"
)

var version = "XX.X.XXXX"

func main() {
	fc := defineFlags(version)

	switch fc.active {
	case "version":
		fmt.Println(version)
	case "createCluster":
		if err := manager.CreateCluster([]string{fc.managerAddress}, fc.createCluster); err != nil {
			fmt.Printf("ERROR: %s\n", err.Error())
			os.Exit(10)
		}
		fmt.Println("ok.")
	case "deleteCluster":
		fmt.Println("CAUTION: The deletion of cluster will create data inconsistency and DATA LOST!")
		fmt.Print("Do you want to continue? (y/N) ")
		reader := bufio.NewReader(os.Stdin)
		char, _, err := reader.ReadRune()
		if err != nil {
			fmt.Println(err)
		}

		switch char {
		case 'Y', 'y':
			if err := manager.DeleteCluster([]string{fc.managerAddress}, fc.deleteCluster); err != nil {
				fmt.Printf("ERROR: %s\n", err.Error())
				os.Exit(20)
			}
			fmt.Println("ok.")
		default:
			fmt.Println("cluster deletion is canceled")
		}
	case "addNode":
		if err := manager.AddNode([]string{fc.managerAddress}, fc.addNode.clusterId, fc.addNode.addresses); err != nil {
			fmt.Printf("ERROR: %s\n", err.Error())
			os.Exit(30)
		}
		fmt.Println("ok.")
	case "removeNode":
		fmt.Println("You are about to remove the node from cluster")
		fmt.Print("Do you want to continue? (y/N) ")
		reader := bufio.NewReader(os.Stdin)
		char, _, err := reader.ReadRune()
		if err != nil {
			fmt.Println(err)
		}

		switch char {
		case 'Y', 'y':
			if err := manager.RemoveNode([]string{fc.managerAddress}, fc.removeNode); err != nil {
				fmt.Printf("ERROR: %s\n", err.Error())
				os.Exit(40)
			}
			fmt.Println("ok.")
		default:
			fmt.Println("node removal is canceled")
		}
	case "syncClusters":
		anim := common.NewAnimation("clusters are in sync process...")
		anim.Start()

		if err := manager.SyncClusters([]string{fc.managerAddress}); err != nil {
			anim.Cancel()
			fmt.Printf("%s\n", err.Error())
			os.Exit(50)
		}
		anim.Stop()
	case "getCluster":
		if err := manager.GetClusters([]string{fc.managerAddress}, fc.getCluster); err != nil {
			fmt.Printf("%s\n", err.Error())
			os.Exit(60)
		}
		fmt.Println("ok.")
	case "getClusters":
		if err := manager.GetClusters([]string{fc.managerAddress}, ""); err != nil {
			fmt.Printf("%s\n", err.Error())
			os.Exit(60)
		}
		fmt.Println("ok.")
	}
}
