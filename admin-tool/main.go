package main

import (
	"bufio"
	"fmt"
	"os"

	"github.com/freakmaxi/kertish-dfs/admin-tool/manager"
	"github.com/freakmaxi/kertish-dfs/basics/common"
	"github.com/freakmaxi/kertish-dfs/basics/terminal"
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
	case "moveCluster":
		fmt.Println("CAUTION: The cluster move can be partially done and clusters may need to be manually unfrozen!")
		fmt.Print("Do you want to continue? (y/N) ")
		reader := bufio.NewReader(os.Stdin)
		char, _, err := reader.ReadRune()
		if err != nil {
			fmt.Println(err)
		}

		switch char {
		case 'Y', 'y':
			anim := common.NewAnimation(terminal.NewStdOut(), "clusters are in move process...")
			anim.Start()

			if err := manager.MoveCluster([]string{fc.managerAddress}, fc.moveCluster); err != nil {
				anim.Cancel()

				fmt.Printf("ERROR: %s\n", err.Error())
				os.Exit(25)
			}
			anim.Stop()
		default:
			fmt.Println("cluster move is canceled")
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
	case "unFreeze":
		if err := manager.Unfreeze([]string{fc.managerAddress}, fc.unfreeze); err != nil {
			fmt.Printf("ERROR: %s\n", err.Error())
			os.Exit(45)
		}
		fmt.Println("ok.")
	case "syncClusters":
		fmt.Println("CAUTION: The sync of clusters will be started simultaneously on each cluster and it will " +
			"prevent the access to the syncing cluster for read, write and delete operations!")
		fmt.Print("Do you want to continue? (y/N) ")
		reader := bufio.NewReader(os.Stdin)
		char, _, err := reader.ReadRune()
		if err != nil {
			fmt.Println(err)
		}

		switch char {
		case 'Y', 'y':
			anim := common.NewAnimation(terminal.NewStdOut(), "clusters are in sync process...")
			anim.Start()

			if err := manager.SyncClusters([]string{fc.managerAddress}); err != nil {
				anim.Cancel()

				fmt.Printf("%s\n", err.Error())
				os.Exit(50)
			}
			anim.Stop()
		default:
			fmt.Println("cluster sync is canceled")
		}
	case "repairConsistency":
		fmt.Println("CAUTION: Repair consistency will prevent access to all clusters for any kind of actions. It is " +
			"a long running process and it may take hours/days to complete depending on your DFS setup and Repair model.")
		fmt.Print("Do you want to continue? (y/N) ")
		reader := bufio.NewReader(os.Stdin)
		char, _, err := reader.ReadRune()
		if err != nil {
			fmt.Println(err)
		}

		switch char {
		case 'Y', 'y':
			anim := common.NewAnimation(terminal.NewStdOut(), "metadata file chunk consistency repair is in progress...")
			anim.Start()

			if err := manager.RepairConsistency([]string{fc.managerAddress}, fc.repairConsistency); err != nil {
				anim.Cancel()

				fmt.Printf("%s\n", err.Error())
				os.Exit(55)
			}
			anim.Stop()
		default:
			fmt.Println("cluster chunk consistency repair is canceled")
		}
	case "balanceClusters":
		fmt.Println("CAUTION: Balancing process will prevent access to the balancing clusters for any kind of actions. " +
			"It is a long running process and it may take hours/days to complete depending on the internet speed between balancing clusters " +
			"and the size of them. Clusters may need to be manually unfrozen!")
		fmt.Print("Do you want to continue? (y/N) ")
		reader := bufio.NewReader(os.Stdin)
		char, _, err := reader.ReadRune()
		if err != nil {
			fmt.Println(err)
		}

		switch char {
		case 'Y', 'y':
			anim := common.NewAnimation(terminal.NewStdOut(), "cluster balancing is in progress...")
			anim.Start()

			if err := manager.BalanceClusters([]string{fc.managerAddress}, fc.balanceClusters); err != nil {
				anim.Cancel()

				fmt.Printf("%s\n", err.Error())
				os.Exit(60)
			}
			anim.Stop()
		default:
			fmt.Println("cluster balancing is canceled")
		}
	case "getCluster":
		if err := manager.GetClusters([]string{fc.managerAddress}, fc.getCluster); err != nil {
			fmt.Printf("%s\n", err.Error())
			os.Exit(65)
		}
		fmt.Println("ok.")
	case "getClusters":
		if err := manager.GetClusters([]string{fc.managerAddress}, ""); err != nil {
			fmt.Printf("%s\n", err.Error())
			os.Exit(70)
		}
		fmt.Println("ok.")
	}
}
