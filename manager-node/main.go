package main

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/freakmaxi/kertish-dfs/manager-node/data"
	"github.com/freakmaxi/kertish-dfs/manager-node/manager"
	"github.com/freakmaxi/kertish-dfs/manager-node/routing"
	"github.com/freakmaxi/kertish-dfs/manager-node/services"
	"github.com/freakmaxi/locking-center-client-go/mutex"
)

var version = "XX.X.XXXX"

func main() {
	printWelcome()

	args := os.Args[1:]
	if len(args) > 0 && strings.Compare(args[0], "--version") == 0 {
		fmt.Println(version)
		return
	}

	fmt.Println("INFO: ---------- Starting Manager Node -----------")

	bindAddr := os.Getenv("BIND_ADDRESS")
	if len(bindAddr) == 0 {
		bindAddr = ":9400"
	}
	fmt.Printf("INFO: BIND_ADDRESS: %s\n", bindAddr)

	healthTrackerIntervalString := os.Getenv("HEALTH_TRACKER_INTERVAL")
	if len(healthTrackerIntervalString) == 0 {
		healthTrackerIntervalString = "10"
	}
	healthTrackerInterval, err := strconv.ParseUint(healthTrackerIntervalString, 10, 64)
	if err != nil {
		fmt.Printf("ERROR: Health Tracker Interval is wrong: %s\n", err.Error())
		os.Exit(5)
	}
	if healthTrackerInterval > 0 {
		fmt.Printf("INFO: HEALTH_TRACKER_INTERVAL: %s second(s)\n", healthTrackerIntervalString)
	}

	mongoConn := os.Getenv("MONGO_CONN")
	if len(mongoConn) == 0 {
		fmt.Println("ERROR: MONGO_CONN have to be specified")
		os.Exit(10)
	}
	fmt.Printf("INFO: MONGO_CONN: %s\n", mongoConn)

	mongoDb := os.Getenv("MONGO_DATABASE")
	if len(mongoDb) == 0 {
		mongoDb = "kertish-dfs"
	}
	fmt.Printf("INFO: MONGO_DATABASE: %s\n", mongoDb)

	redisConn := os.Getenv("REDIS_CONN")
	if len(redisConn) == 0 {
		fmt.Println("ERROR: REDIS_CONN have to be specified")
		os.Exit(11)
	}
	fmt.Printf("INFO: REDIS_CONN: %s\n", redisConn)

	redisPassword := os.Getenv("REDIS_PASSWORD")
	fmt.Printf("INFO: REDIS_PASSWORD: %t\n", len(redisPassword) > 0)

	redisClusterMode := os.Getenv("REDIS_CLUSTER_MODE")
	fmt.Printf("INFO: REDIS_CLUSTER_MODE: %t\n", len(redisClusterMode) > 0)

	mutexConn := os.Getenv("LOCKING_CENTER")
	if len(mutexConn) == 0 {
		fmt.Println("ERROR: LOCKING_CENTER have to be specified")
		os.Exit(15)
	}
	fmt.Printf("INFO: LOCKING_CENTER: %s\n", mutexConn)

	m, err := mutex.NewLockingCenter(mutexConn)
	if err != nil {
		fmt.Printf("ERROR: Mutex Setup is failed. %s\n", err.Error())
		os.Exit(20)
	}

	conn, err := data.NewConnection(mongoConn)
	if err != nil {
		fmt.Printf("ERROR: MongoDB Connection is failed. %s\n", err.Error())
		os.Exit(21)
	}

	dataClusters, err := data.NewClusters(conn, mongoDb, m)
	if err != nil {
		fmt.Printf("ERROR: Cluster Data Manager is failed. %s\n", err.Error())
		os.Exit(22)
	}

	var indexClient data.IndexClient
	if len(redisClusterMode) == 0 {
		indexClient, err = data.NewIndexStandaloneClient(redisConn, redisPassword)
	} else {
		indexClient, err = data.NewIndexClusterClient(strings.Split(redisConn, ","), redisPassword)
	}
	if err != nil {
		fmt.Printf("ERROR: Index Setup is failed. %s\n", err.Error())
		os.Exit(23)
	}
	index := data.NewIndex(indexClient, strings.ReplaceAll(mongoDb, " ", "_"))

	metadata, err := data.NewMetadata(m, conn, mongoDb)
	if err != nil {
		fmt.Printf("ERROR: Metadata Manager is failed. %s\n", err.Error())
		os.Exit(24)
	}

	routerManager := routing.NewManager()

	managerCluster, err := manager.NewCluster(dataClusters, index, metadata)
	if err != nil {
		fmt.Printf("ERROR: Cluster Manager is failed. %s\n", err.Error())
		os.Exit(25)
	}

	// No need to block start up with cluster sync
	go func() {
		fmt.Print("INFO: Syncing Clusters...\n")
		errorList := managerCluster.SyncClusters()
		if len(errorList) > 0 {
			for _, err := range errorList {
				fmt.Printf("ERROR: Sync is failed! %s\n", err.Error())
			}
			return
		}
		fmt.Print("INFO: Sync is done!\n")
	}()

	managerRouter := routing.NewManagerRouter(managerCluster)
	routerManager.Add(managerRouter)

	managerNode := manager.NewNode(index, dataClusters)
	nodeRouter := routing.NewNodeRouter(managerNode)
	routerManager.Add(nodeRouter)

	healthTracker := manager.NewHealthTracker(dataClusters, index, time.Second*time.Duration(healthTrackerInterval))
	healthTracker.Start()

	proxy := services.NewProxy(bindAddr, routerManager)
	proxy.Start()

	os.Exit(0)
}

func printWelcome() {
	fmt.Println()
	fmt.Println("     'o@@@@@@o,  o@@@@@@o")
	fmt.Println("   'o@@@@o/-\\@@|@@/--\\@@@o             __ _  ____  ____  ____  __  ____  _  _")
	fmt.Println("  `o@/.       `@@~      o@@o          (  / )(  __)(  _ \\(_  _)(  )/ ___)/ )( \\")
	fmt.Println("  o@@:   oo    @@ .@@@. :@@~           )  (  ) _)  )   /  )(   )( \\___ \\) __ (")
	fmt.Println("  o@@,  .@@@.  @@=  oo  o@o`          (__\\_)(____)(__\\_) (__) (__)(____/\\_)(_/")
	fmt.Println("  '@@%`      `@@@@o....@@%`                                  ____  ____  ____")
	fmt.Println("   :@@@@o....@@@@@@@@@@@@@%~                                (    \\(  __)/ ___)")
	fmt.Println(" .oo@@@@@@@@@@@@@@@@@@@@@@@@o~`    .@@@@`                    ) D ( ) _) \\___ \\")
	fmt.Println("o@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@o . @@@oo@@@@@               (____/(__)  (____/")
	fmt.Printf("o@@@@@@%%@@@@@@@@@@@@@@@@@@@@@@@@@@@@@`  @o  @               version %s\n", version)
	fmt.Println("o@@@@@@:~O@@@@@@@@@@@@@@@@@@@@@@@@@@@ooo@@@@@")
	fmt.Println(" ~o@@@@|  `O@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@/`")
	fmt.Println("   `~=/`   *O@@@@@@@@@@@@@@@@@@@@@@@@@@@O/")
	fmt.Println("              \\\\O@@@@@@@@@@@@@@@@@@@@@O/`")
	fmt.Println("                 `\\\\|O@@@@@@@@@0oo/:")
	fmt.Println()
	fmt.Printf("Visit: https://github.com/freakmaxi/kertish-dfs\n")
	fmt.Println()
}
