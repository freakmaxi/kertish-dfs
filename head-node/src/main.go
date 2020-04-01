package main

import (
	"fmt"
	"os"
	"strings"

	"github.com/freakmaxi/kertish-dfs/head-node/src/data"
	"github.com/freakmaxi/kertish-dfs/head-node/src/manager"
	"github.com/freakmaxi/kertish-dfs/head-node/src/routing"
	"github.com/freakmaxi/kertish-dfs/head-node/src/services"
)

var version = "XX.X.XXXX"

func main() {
	printWelcome()

	args := os.Args[1:]
	if len(args) > 0 && strings.Compare(args[0], "--version") == 0 {
		fmt.Println(version)
		return
	}

	fmt.Println("INFO: ------------ Starting Head Node ------------")

	bindAddr := os.Getenv("BIND_ADDRESS")
	if len(bindAddr) == 0 {
		bindAddr = ":4000"
	}
	fmt.Printf("INFO: BIND_ADDRESS: %s\n", bindAddr)

	managerAddress := os.Getenv("MANAGER_ADDRESS")
	if len(managerAddress) == 0 {
		fmt.Println("ERROR: MANAGER_ADDRESS have to be specified")
		os.Exit(10)
	}
	fmt.Printf("INFO: MANAGER_ADDRESS: %s\n", managerAddress)

	mongoConn := os.Getenv("MONGO_CONN")
	if len(mongoConn) == 0 {
		fmt.Println("ERROR: MONGO_CONN have to be specified")
		os.Exit(11)
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
		os.Exit(12)
	}
	fmt.Printf("INFO: REDIS_CONN: %s\n", redisConn)

	redisPassword := os.Getenv("REDIS_PASSWORD")
	fmt.Printf("INFO: REDIS_PASSWORD: %t\n", len(redisPassword) > 0)

	redisClusterMode := os.Getenv("REDIS_CLUSTER_MODE")
	fmt.Printf("INFO: REDIS_CLUSTER_MODE: %t\n", len(redisClusterMode) > 0)

	var mutexClient data.MutexClient
	var err error
	if len(redisClusterMode) == 0 {
		mutexClient, err = data.NewMutexStandaloneClient(redisConn, redisPassword)
	} else {
		mutexClient, err = data.NewMutexClusterClient(strings.Split(redisConn, ","), redisPassword)
	}
	if err != nil {
		fmt.Printf("ERROR: Mutex Setup is failed. %s\n", err.Error())
		os.Exit(13)
	}
	mutex := data.NewMutex(mutexClient)

	conn, err := data.NewConnection(mongoConn)
	if err != nil {
		fmt.Printf("ERROR: MongoDB Connection is failed. %s\n", err.Error())
		os.Exit(15)
	}

	metadata, err := data.NewMetadata(mutex, conn, mongoDb)
	if err != nil {
		fmt.Printf("ERROR: Metadata Manager is failed. %s\n", err.Error())
		os.Exit(18)
	}

	cluster, err := manager.NewCluster([]string{managerAddress})
	if err != nil {
		fmt.Printf("ERROR: Cluster Manager is failed. %s\n", err.Error())
		os.Exit(20)
	}
	dfs := manager.NewDfs(metadata, cluster)
	// create root if not exists
	if err := dfs.CreateFolder("/"); err != nil && err != os.ErrExist {
		fmt.Printf("ERROR: Unable to create cluster root path. %s\n", err.Error())
		os.Exit(21)
	}
	dfsRouter := routing.NewDfsRouter(dfs)

	routerManager := routing.NewManager()
	routerManager.Add(dfsRouter)

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
