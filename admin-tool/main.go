package main

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/freakmaxi/kertish-dfs/admin-tool/manager"
	"github.com/freakmaxi/kertish-dfs/basics/common"
	"github.com/freakmaxi/kertish-dfs/basics/terminal"
)

var version = "XX.X.XXXX"
var build = "XXXXXX"

func main() {
	fc := defineFlags(version, build)

	switch fc.active {
	case "version", "v":
		fmt.Printf("%s-%s\n", version, build)
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
	case "createSnapshot":
		fmt.Println("CAUTION: The snapshot creation will prevent the access to the cluster!")
		fmt.Print("Do you want to continue? (y/N) ")
		reader := bufio.NewReader(os.Stdin)
		char, _, err := reader.ReadRune()
		if err != nil {
			fmt.Println(err)
		}

		switch char {
		case 'Y', 'y':
			anim := common.NewAnimation(terminal.NewStdOut(), "creating snapshot...")
			anim.Start()

			if err := manager.CreateSnapshot([]string{fc.managerAddress}, fc.createSnapshot); err != nil {
				anim.Cancel()

				fmt.Printf("%s\n", err.Error())
				os.Exit(75)
			}
			anim.Stop()
		default:
			fmt.Println("snapshot creation is canceled")
		}
	case "deleteSnapshot":
		fmt.Println("CAUTION: You are about to delete the snapshot")
		fmt.Print("Do you want to continue? (y/N) ")
		reader := bufio.NewReader(os.Stdin)
		char, _, err := reader.ReadRune()
		if err != nil {
			fmt.Println(err)
		}

		switch char {
		case 'Y', 'y':
			anim := common.NewAnimation(terminal.NewStdOut(), "deleting snapshot...")
			anim.Start()

			eqIdx := strings.Index(fc.deleteSnapshot, "=")
			clusterId := fc.deleteSnapshot[:eqIdx]
			snapshotIndex, _ := strconv.ParseUint(fc.deleteSnapshot[eqIdx+1:], 10, 64)

			if err := manager.DeleteSnapshot([]string{fc.managerAddress}, clusterId, snapshotIndex); err != nil {
				anim.Cancel()

				fmt.Printf("%s\n", err.Error())
				os.Exit(50)
			}
			anim.Stop()
		default:
			fmt.Println("snapshot deletion is canceled")
		}
	case "restoreSnapshot":
		fmt.Println("CAUTION: When you restore the snapshot, current state of the data will be reverted to the snapshot point.")
		fmt.Print("Do you want to continue? (y/N) ")
		reader := bufio.NewReader(os.Stdin)
		char, _, err := reader.ReadRune()
		if err != nil {
			fmt.Println(err)
		}

		switch char {
		case 'Y', 'y':
			anim := common.NewAnimation(terminal.NewStdOut(), "restoring snapshot...")
			anim.Start()

			eqIdx := strings.Index(fc.restoreSnapshot, "=")
			clusterId := fc.restoreSnapshot[:eqIdx]
			snapshotIndex, _ := strconv.ParseUint(fc.restoreSnapshot[eqIdx+1:], 10, 64)

			if err := manager.RestoreSnapshot([]string{fc.managerAddress}, clusterId, snapshotIndex); err != nil {
				anim.Cancel()

				fmt.Printf("%s\n", err.Error())
				os.Exit(50)
			}
			anim.Stop()
		default:
			fmt.Println("snapshot restoration is canceled")
		}
	case "syncClusters":
		if fc.syncClusters {
			fmt.Println("CAUTION: The sync of clusters will be started simultaneously on each cluster and it will " +
				"prevent the access to the syncing cluster for read, write and delete operations!")
		} else {
			fmt.Println("CAUTION: The sync of cluster will prevent the access to the syncing cluster for read, write and delete operations!")
		}
		fmt.Print("Do you want to continue? (y/N) ")
		reader := bufio.NewReader(os.Stdin)
		char, _, err := reader.ReadRune()
		if err != nil {
			fmt.Println(err)
		}

		switch char {
		case 'Y', 'y':
			if err := manager.SyncClusters([]string{fc.managerAddress}, fc.syncCluster, fc.force); err != nil {
				fmt.Printf("%s\n", err.Error())
				os.Exit(50)
			}
			fmt.Println("cluster sync is started, you can check the state with --get-clusters option")
		default:
			fmt.Println("cluster sync is canceled")
		}
	case "clustersReport":
		if err := manager.GetReport([]string{fc.managerAddress}); err != nil {
			fmt.Printf("%s\n", err.Error())
			os.Exit(80)
		}
		fmt.Println("ok.")
	case "repairConsistency":
		fmt.Println("CAUTION: Repair consistency is a long running process that may take hours/days to complete " +
			"depending on your DFS setup and will create partial action prevention on cluster data nodes.")
		fmt.Print("Do you want to continue? (y/N) ")
		reader := bufio.NewReader(os.Stdin)
		char, _, err := reader.ReadRune()
		if err != nil {
			fmt.Println(err)
		}

		switch char {
		case 'Y', 'y':
			if err := manager.RepairConsistency([]string{fc.managerAddress}, fc.repairConsistency); err != nil {
				fmt.Printf("%s\n", err.Error())
				os.Exit(55)
			}
			fmt.Println("consistency repair is started, you can check the state with --get-clusters option")
		default:
			fmt.Println("cluster chunk consistency repair is canceled")
		}
	case "balanceClusters":
		fmt.Println("CAUTION: Balancing is a long running process and it may take hours/days to complete depending " +
			"on the internet speed between balancing clusters and the size of them.")
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
