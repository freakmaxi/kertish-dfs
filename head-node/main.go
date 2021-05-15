package main

import (
	"fmt"
	"os"
	"strings"

	"github.com/freakmaxi/kertish-dfs/basics/hooks"
	"github.com/freakmaxi/kertish-dfs/basics/logging"
	"github.com/freakmaxi/kertish-dfs/head-node/data"
	"github.com/freakmaxi/kertish-dfs/head-node/manager"
	"github.com/freakmaxi/kertish-dfs/head-node/routing"
	"github.com/freakmaxi/kertish-dfs/head-node/services"
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

	logger, console := logging.NewLogger("head")
	defer func() { _ = logger.Sync() }()

	printWelcome(console)

	logger.Info("------------ Starting Head Node ------------")

	bindAddr := os.Getenv("BIND_ADDRESS")
	if len(bindAddr) == 0 {
		bindAddr = ":4000"
	}
	logger.Info(fmt.Sprintf("BIND_ADDRESS: %s", bindAddr))

	mutexSourceAddr := bindAddr
	if strings.Index(mutexSourceAddr, ":") == 0 {
		mutexSourceAddr = fmt.Sprintf("127.0.0.1%s", mutexSourceAddr)
	}

	managerAddress := os.Getenv("MANAGER_ADDRESS")
	if len(managerAddress) == 0 {
		logger.Error("MANAGER_ADDRESS have to be specified")
		os.Exit(10)
	}
	logger.Info(fmt.Sprintf("MANAGER_ADDRESS: %s", managerAddress))

	hooks.CurrentLoader = hooks.NewLoader(os.Getenv("HOOKS_PATH"), logger)
	logger.Info(fmt.Sprintf("HOOKS_PATH: %s", hooks.CurrentLoader.HooksPath()))

	mongoConn := os.Getenv("MONGO_CONN")
	if len(mongoConn) == 0 {
		logger.Error("MONGO_CONN have to be specified")
		os.Exit(11)
	}
	logger.Info(fmt.Sprintf("MONGO_CONN: %s", mongoConn))

	mongoDb := os.Getenv("MONGO_DATABASE")
	if len(mongoDb) == 0 {
		mongoDb = "kertish-dfs"
	}
	logger.Info(fmt.Sprintf("MONGO_DATABASE: %s", mongoDb))

	mongoTransaction := os.Getenv("MONGO_TRANSACTION")
	logger.Info(fmt.Sprintf("MONGO_TRANSACTION: %t", len(mongoTransaction) > 0))

	mutexConn := os.Getenv("LOCKING_CENTER")
	if len(mutexConn) == 0 {
		logger.Error("LOCKING_CENTER have to be specified")
		os.Exit(13)
	}
	logger.Info(fmt.Sprintf("LOCKING_CENTER: %s", mutexConn))

	m, err := mutex.NewLockingCenterWithSourceAddr(mutexConn, &mutexSourceAddr)
	if err != nil {
		logger.Error("Mutex Setup is failed", zap.Error(err))
		os.Exit(14)
	}
	m.ResetBySource(&mutexSourceAddr)

	conn, err := data.NewConnection(mongoConn, len(mongoTransaction) > 0)
	if err != nil {
		logger.Error("MongoDB Connection is failed", zap.Error(err))
		os.Exit(15)
	}

	metadata, err := data.NewMetadata(m, conn, mongoDb)
	if err != nil {
		logger.Error("Metadata Manager is failed", zap.Error(err))
		os.Exit(18)
	}

	cluster, err := manager.NewCluster([]string{managerAddress}, logger)
	if err != nil {
		logger.Error("Cluster Manager is failed", zap.Error(err))
		os.Exit(20)
	}
	dfs := manager.NewDfs(metadata, cluster, logger)
	// create root if not exists
	if err := dfs.CreateFolder("/"); err != nil && err != os.ErrExist {
		logger.Error("Unable to create cluster root path", zap.Error(err))
		os.Exit(21)
	}
	dfsRouter := routing.NewDfsRouter(dfs, logger)

	hook := manager.NewHook(metadata, logger)
	hookRouter := routing.NewHookRouter(hook, logger)

	routerManager := routing.NewManager()
	routerManager.Add(dfsRouter)
	routerManager.Add(hookRouter)

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
