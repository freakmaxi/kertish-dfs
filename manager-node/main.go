package main

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/freakmaxi/kertish-dfs/basics/logging"
	"github.com/freakmaxi/kertish-dfs/manager-node/data"
	"github.com/freakmaxi/kertish-dfs/manager-node/manager"
	"github.com/freakmaxi/kertish-dfs/manager-node/routing"
	"github.com/freakmaxi/kertish-dfs/manager-node/services"
	"github.com/freakmaxi/locking-center-client-go/mutex"
	"go.uber.org/zap"
)

var version = "XX.X.XXXX"

func main() {
	args := os.Args[1:]
	if len(args) > 0 && strings.Compare(args[0], "--version") == 0 {
		fmt.Println(version)
		return
	}

	logger, console := logging.NewLogger("manager")
	defer func() { _ = logger.Sync() }()

	printWelcome(console)

	logger.Info("---------- Starting Manager Node -----------")

	bindAddr := os.Getenv("BIND_ADDRESS")
	if len(bindAddr) == 0 {
		bindAddr = ":9400"
	}
	logger.Info(fmt.Sprintf("BIND_ADDRESS: %s", bindAddr))

	mutexSourceAddr := bindAddr
	if strings.Index(mutexSourceAddr, ":") == 0 {
		mutexSourceAddr = fmt.Sprintf("127.0.0.1%s", mutexSourceAddr)
	}

	healthCheckIntervalString := os.Getenv("HEALTH_CHECK_INTERVAL")
	if len(healthCheckIntervalString) == 0 {
		healthCheckIntervalString = "10"
	}
	healthCheckInterval, err := strconv.ParseUint(healthCheckIntervalString, 10, 64)
	if err != nil {
		logger.Error("Health Check Interval is wrong", zap.Error(err))
		os.Exit(5)
	}
	if healthCheckInterval > 0 {
		logger.Info(fmt.Sprintf("HEALTH_CHECK_INTERVAL: %s second(s)", healthCheckIntervalString))
	}

	mongoConn := os.Getenv("MONGO_CONN")
	if len(mongoConn) == 0 {
		logger.Error("MONGO_CONN have to be specified")
		os.Exit(10)
	}
	logger.Info(fmt.Sprintf("MONGO_CONN: %s", mongoConn))

	mongoDb := os.Getenv("MONGO_DATABASE")
	if len(mongoDb) == 0 {
		mongoDb = "kertish-dfs"
	}
	logger.Info(fmt.Sprintf("MONGO_DATABASE: %s", mongoDb))

	mongoTransaction := os.Getenv("MONGO_TRANSACTION")
	logger.Info(fmt.Sprintf("MONGO_TRANSACTION: %t", len(mongoTransaction) > 0))

	redisConn := os.Getenv("REDIS_CONN")
	if len(redisConn) == 0 {
		logger.Error("REDIS_CONN have to be specified")
		os.Exit(11)
	}
	logger.Info(fmt.Sprintf("REDIS_CONN: %s", redisConn))

	redisPassword := os.Getenv("REDIS_PASSWORD")
	logger.Info(fmt.Sprintf("REDIS_PASSWORD: %t", len(redisPassword) > 0))

	redisTimeoutString := os.Getenv("REDIS_TIMEOUT")
	if len(redisTimeoutString) == 0 {
		redisTimeoutString = "0"
	}
	redisTimeout, err := strconv.ParseUint(redisTimeoutString, 10, 64)
	if err != nil {
		logger.Error("Redis timeout value is wrong", zap.Error(err))
		os.Exit(12)
	}
	if redisTimeout > 0 {
		logger.Info(fmt.Sprintf("REDIS_TIMEOUT: %s second(s)", redisTimeoutString))
	}

	redisClusterMode := os.Getenv("REDIS_CLUSTER_MODE")
	logger.Info(fmt.Sprintf("REDIS_CLUSTER_MODE: %t", len(redisClusterMode) > 0))

	mutexConn := os.Getenv("LOCKING_CENTER")
	if len(mutexConn) == 0 {
		logger.Error("LOCKING_CENTER have to be specified")
		os.Exit(15)
	}
	logger.Info(fmt.Sprintf("LOCKING_CENTER: %s", mutexConn))

	m, err := mutex.NewLockingCenterWithSourceAddr(mutexConn, &mutexSourceAddr)
	if err != nil {
		logger.Error("Mutex Setup is failed", zap.Error(err))
		os.Exit(20)
	}
	m.ResetBySource(&mutexSourceAddr)

	conn, err := data.NewConnection(mongoConn, len(mongoTransaction) > 0)
	if err != nil {
		logger.Error("MongoDB Connection is failed", zap.Error(err))
		os.Exit(21)
	}

	dataClusters, err := data.NewClusters(conn, mongoDb, m)
	if err != nil {
		logger.Error("Cluster Data Manager is failed", zap.Error(err))
		os.Exit(22)
	}

	var cacheClient data.CacheClient
	if len(redisClusterMode) == 0 {
		cacheClient, err = data.NewCacheStandaloneClient(redisConn, redisPassword, redisTimeout)
	} else {
		cacheClient, err = data.NewCacheClusterClient(strings.Split(redisConn, ","), redisPassword, redisTimeout)
	}
	if err != nil {
		logger.Error("Cache Client Setup is failed", zap.Error(err))
		os.Exit(23)
	}
	index := data.NewIndex(cacheClient, strings.ReplaceAll(mongoDb, " ", "_"), logger)
	operation := data.NewOperation(cacheClient, strings.ReplaceAll(mongoDb, " ", "_"))

	metadata, err := data.NewMetadata(m, conn, mongoDb)
	if err != nil {
		logger.Error("Metadata Manager is failed", zap.Error(err))
		os.Exit(24)
	}

	synchronize := manager.NewSynchronize(dataClusters, index, logger)
	repair := manager.NewRepair(dataClusters, metadata, index, operation, synchronize, logger)

	health := manager.NewHealthTracker(dataClusters, index, synchronize, repair, logger, time.Second*time.Duration(healthCheckInterval))
	health.Start()

	managerCluster, err := manager.NewCluster(dataClusters, index, synchronize, logger)
	if err != nil {
		logger.Error("Cluster Manager is failed", zap.Error(err))
		os.Exit(25)
	}
	managerRouter := routing.NewManagerRouter(managerCluster, synchronize, repair, health, logger)

	if err := managerCluster.Handshake(); err != nil {
		logger.Error("Handshake is failed with cluster nodes", zap.Error(err))
	} else {
		logger.Info("Handshake is completed with cluster nodes...")
	}

	routerManager := routing.NewManager()
	routerManager.Add(managerRouter)

	managerNode := manager.NewNode(dataClusters, index, logger)
	nodeRouter := routing.NewNodeRouter(managerNode, logger)
	routerManager.Add(nodeRouter)

	proxy := services.NewProxy(bindAddr, routerManager, logger)
	proxy.Start()

	os.Exit(0)
}

func printWelcome(console bool) {
	if !console {
		fmt.Printf("Kertish DFS, version %s\n", version)
		fmt.Printf("Visit: https://github.com/freakmaxi/kertish-dfs\n")
		return
	}

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
