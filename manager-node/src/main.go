package main

import (
	"fmt"
	"os"
	"strings"

	"github.com/freakmaxi/2020-dfs/manager-node/src/data"
	"github.com/freakmaxi/2020-dfs/manager-node/src/manager"
	"github.com/freakmaxi/2020-dfs/manager-node/src/routing"
	"github.com/freakmaxi/2020-dfs/manager-node/src/services"
)

var version = "XX.X.XXXX"

func main() {
	args := os.Args[1:]
	if len(args) > 0 && strings.Compare(args[0], "--version") == 0 {
		fmt.Println(version)
		return
	}

	fmt.Printf("INFO: Starting 2020-dfs Manager Node v%s\n", version)

	bindAddr := os.Getenv("BIND_ADDRESS")
	if len(bindAddr) == 0 {
		bindAddr = ":9400"
	}
	fmt.Printf("INFO: BIND_ADDRESS: %s\n", bindAddr)

	mongoConn := os.Getenv("MONGO_CONN")
	if len(mongoConn) == 0 {
		fmt.Println("ERROR: MONGO_CONN have to be specified")
		os.Exit(10)
	}
	fmt.Printf("INFO: MONGO_CONN: %s\n", mongoConn)

	mongoDb := os.Getenv("MONGO_DATABASE")
	if len(mongoDb) == 0 {
		mongoDb = "2020-dfs"
	}
	fmt.Printf("INFO: MONGO_DATABASE: %s\n", mongoDb)

	redisConn := os.Getenv("REDIS_CONN")
	if len(redisConn) == 0 {
		fmt.Println("ERROR: REDIS_CONN have to be specified")
		os.Exit(11)
	}
	fmt.Printf("INFO: REDIS_CONN: %s\n", redisConn)

	conn, err := data.NewConnection(mongoConn)
	if err != nil {
		fmt.Printf("ERROR: MongoDB Connection is failed. %s\n", err.Error())
		os.Exit(20)
	}

	mutex, err := data.NewMutex(redisConn)
	if err != nil {
		fmt.Printf("ERROR: Mutex Setup is failed. %s\n", err.Error())
		os.Exit(21)
	}

	index, err := data.NewIndex(redisConn, strings.ReplaceAll(mongoDb, " ", "_"), mutex)
	if err != nil {
		fmt.Printf("ERROR: Index Setup is failed. %s\n", err.Error())
		os.Exit(22)
	}

	dataClusters, err := data.NewClusters(mutex, conn, mongoDb)
	if err != nil {
		fmt.Printf("ERROR: Cluster Data Manager is failed. %s\n", err.Error())
		os.Exit(23)
	}

	managerCluster, err := manager.NewCluster(index, dataClusters)
	if err != nil {
		fmt.Printf("ERROR: Cluster Manager is failed. %s\n", err.Error())
		os.Exit(24)
	}
	if err := managerCluster.SyncClusters(); err != nil {
		fmt.Printf("ERROR: Cluster Syncing is failed. %s\n", err.Error())
	}
	managerRouter := routing.NewManagerRouter(managerCluster)

	managerNode, err := manager.NewNode(index, dataClusters)
	if err != nil {
		fmt.Printf("ERROR: Node Manager is failed. %s\n", err.Error())
		os.Exit(24)
	}
	nodeRouter := routing.NewNodeRouter(managerNode)

	routerManager := routing.NewManager()
	routerManager.Add(managerRouter)
	routerManager.Add(nodeRouter)

	proxy := services.NewProxy(bindAddr, routerManager)
	proxy.Start()

	os.Exit(0)
}
