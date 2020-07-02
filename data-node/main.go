package main

import (
	"fmt"
	"net"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/freakmaxi/kertish-dfs/basics/log"
	"github.com/freakmaxi/kertish-dfs/data-node/cache"
	"github.com/freakmaxi/kertish-dfs/data-node/filesystem"
	"github.com/freakmaxi/kertish-dfs/data-node/manager"
	"github.com/freakmaxi/kertish-dfs/data-node/service"
	"go.uber.org/zap"
)

var version = "XX.X.XXXX"

func main() {
	args := os.Args[1:]
	if len(args) > 0 && strings.Compare(args[0], "--version") == 0 {
		fmt.Println(version)
		return
	}

	logger, console := log.NewLogger("data")
	defer func() { _ = logger.Sync() }()

	if console {
		printWelcome()
	}

	logger.Info("------------ Starting Data Node ------------")

	hardwareAddr, err := findHardwareAddress()
	if err != nil {
		logger.Error("Unable to read hardware details", zap.Error(err))
		os.Exit(1)
	}
	logger.Sugar().Infof("HARDWARE_ID: %s", hardwareAddr)

	bindAddr := os.Getenv("BIND_ADDRESS")
	if matched, err := regexp.MatchString(`:\d{1,5}$`, bindAddr); err != nil || !matched {
		bindAddr = fmt.Sprintf("%s:9430", bindAddr)
	}
	logger.Sugar().Infof("BIND_ADDRESS: %s", bindAddr)

	managerAddress := os.Getenv("MANAGER_ADDRESS")
	if len(managerAddress) == 0 {
		logger.Error("MANAGER_ADDRESS have to be specified")
		os.Exit(10)
	}
	logger.Sugar().Infof("MANAGER_ADDRESS: %s", managerAddress)

	sizeString := os.Getenv("SIZE")
	if len(sizeString) == 0 {
		logger.Error("SIZE have to be specified")
		os.Exit(50)
	}
	size, err := strconv.ParseUint(sizeString, 10, 64)
	if err != nil {
		logger.Error("File System size is wrong", zap.Error(err))
		os.Exit(51)
	}
	if size == 0 {
		logger.Error("File System size can not be 0")
		os.Exit(52)
	}
	logger.Sugar().Infof("SIZE: %s (%s Gb)", sizeString, strconv.FormatUint(size/(1024*1024*1024), 10))

	rootPath := os.Getenv("ROOT_PATH")
	if len(rootPath) == 0 {
		rootPath = "/opt"
	}
	logger.Sugar().Infof("ROOT_PATH: %s", rootPath)

	m, err := filesystem.NewManager(rootPath, logger)
	if err != nil {
		logger.Error("File System Manager creation is failed", zap.Error(err))
		os.Exit(80)
	}
	n := manager.NewNode(strings.Split(managerAddress, ","), size, logger)

	cacheLifetime := 360
	cacheLimitString := os.Getenv("CACHE_LIMIT")
	if len(cacheLimitString) == 0 {
		cacheLimitString = "0"
	}
	cacheLimit, err := strconv.ParseUint(cacheLimitString, 10, 64)
	if err != nil {
		logger.Error("Cache Limit size is wrong", zap.Error(err))
		os.Exit(120)
	}
	if cacheLimit == 0 {
		logger.Warn("Cache is disabled")
	} else {
		logger.Sugar().Infof("CACHE_LIMIT: %s (%s Gb)", cacheLimitString, strconv.FormatUint(cacheLimit/(1024*1024*1024), 10))

		ccLifetimeString := os.Getenv("CACHE_LIFETIME")
		if len(ccLifetimeString) == 0 {
			ccLifetimeString = "360"
		}
		ccLifetime, err := strconv.ParseUint(ccLifetimeString, 10, 64)
		if err != nil {
			logger.Error("Cache Lifetime is wrong", zap.Error(err))
			os.Exit(130)
		}
		if ccLifetime == 0 {
			logger.Error("Cache Lifetime can not be 0")
			os.Exit(131)
		}
		logger.Sugar().Infof("CACHE_LIFETIME: %s min.", ccLifetimeString)
	}

	cc := cache.NewContainer(cacheLimit, time.Minute*time.Duration(cacheLifetime), logger)

	c, err := service.NewCommander(m, cc, n, logger, hardwareAddr)
	if err != nil {
		logger.Error("Commander creation is failed", zap.Error(err))
		os.Exit(200)
	}

	s, err := service.NewServer(bindAddr, c, logger)
	if err != nil {
		logger.Error("Server creation is failed", zap.Error(err))
		os.Exit(300)
	}

	logger.Info("Waiting for handshake...")
	if err := n.Handshake(hardwareAddr, bindAddr, size); err != nil {
		logger.Error("Handshake is failed", zap.Error(err))
		logger.Sugar().Infof("Data Node is starting as stand-alone on %s", bindAddr)
	} else {
		logger.Info("Handshake is successful")

		mode := "MASTER"
		if len(n.MasterAddress()) > 0 {
			mode = "SLAVE"

			go func() {
				if err := m.Sync().Full(n.MasterAddress()); err != nil {
					logger.Warn("Sync is failed", zap.String("masterNodeAddress", n.MasterAddress()), zap.Error(err))
				}
			}()
		}
		logger.Sugar().Infof("Data Node (%s) in Cluster (%s) is starting on %s as %s", n.NodeId(), n.ClusterId(), bindAddr, mode)
	}

	if err := s.Listen(); err != nil {
		logger.Error("Server listening is failed", zap.Error(err))
		os.Exit(400)
	}

	os.Exit(0)
}

func findHardwareAddress() (string, error) {
	interfaces, err := net.Interfaces()
	if err != nil {
		return "", err
	}

	for _, in := range interfaces {
		addrs, err := in.Addrs()
		if err != nil {
			return "", err
		}

		for _, addr := range addrs {
			switch addr := addr.(type) {
			case *net.IPNet:
				addrIp := addr.IP

				if addrIp.To4() == nil || addrIp.IsLoopback() {
					continue
				}

				return in.HardwareAddr.String(), nil
			}
		}
	}
	return "", fmt.Errorf("no suitable hardware address is found")
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
