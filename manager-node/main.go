package main

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/freakmaxi/kertish-dfs/basics/log"
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

	logger, console := log.NewLogger("manager")
	defer func() { _ = logger.Sync() }()

	if console {
		printWelcome()
	}

	logger.Info("---------- Starting Manager Node -----------")

	bindAddr := os.Getenv("BIND_ADDRESS")
	if len(bindAddr) == 0 {
		bindAddr = ":9400"
	}
	logger.Sugar().Infof("BIND_ADDRESS: %s", bindAddr)

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
		logger.Sugar().Infof("HEALTH_CHECK_INTERVAL: %s second(s)", healthCheckIntervalString)
	}

	mongoConn := os.Getenv("MONGO_CONN")
	if len(mongoConn) == 0 {
		logger.Error("MONGO_CONN have to be specified")
		os.Exit(10)
	}
	logger.Sugar().Infof("MONGO_CONN: %s", mongoConn)

	mongoDb := os.Getenv("MONGO_DATABASE")
	if len(mongoDb) == 0 {
		mongoDb = "kertish-dfs"
	}
	logger.Sugar().Infof("MONGO_DATABASE: %s", mongoDb)

	mongoTransaction := os.Getenv("MONGO_TRANSACTION")
	logger.Sugar().Infof("MONGO_TRANSACTION: %t", len(mongoTransaction) > 0)

	redisConn := os.Getenv("REDIS_CONN")
	if len(redisConn) == 0 {
		logger.Error("REDIS_CONN have to be specified")
		os.Exit(11)
	}
	logger.Sugar().Infof("REDIS_CONN: %s", redisConn)

	redisPassword := os.Getenv("REDIS_PASSWORD")
	logger.Sugar().Infof("REDIS_PASSWORD: %t", len(redisPassword) > 0)

	redisClusterMode := os.Getenv("REDIS_CLUSTER_MODE")
	logger.Sugar().Infof("REDIS_CLUSTER_MODE: %t", len(redisClusterMode) > 0)

	mutexConn := os.Getenv("LOCKING_CENTER")
	if len(mutexConn) == 0 {
		logger.Error("LOCKING_CENTER have to be specified")
		os.Exit(15)
	}
	logger.Sugar().Infof("LOCKING_CENTER: %s", mutexConn)

	m, err := mutex.NewLockingCenterWithSourceAddr(mutexConn, &mutexSourceAddr)
	if err != nil {
		logger.Error("Mutex Setup is failed", zap.Error(err))
		os.Exit(20)
	}
	m.ResetBySource(nil)

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
		cacheClient, err = data.NewCacheStandaloneClient(redisConn, redisPassword)
	} else {
		cacheClient, err = data.NewCacheClusterClient(strings.Split(redisConn, ","), redisPassword)
	}
	if err != nil {
		logger.Error("Cache Client Setup is failed", zap.Error(err))
		os.Exit(23)
	}
	index := data.NewIndex(m, cacheClient, strings.ReplaceAll(mongoDb, " ", "_"))
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
