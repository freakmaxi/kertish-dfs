package services

import (
	"fmt"
	"net/http"

	"github.com/freakmaxi/2020-dfs/manager-node/src/routing"
)

type Proxy struct {
	bindAddr string
	manager  *routing.Manager
}

func NewProxy(bindAddr string, manager *routing.Manager) *Proxy {
	return &Proxy{
		bindAddr: bindAddr,
		manager:  manager,
	}
}

func (p *Proxy) Start() {
	fmt.Printf("INFO: 2020-DFS Manager Service is starting on %s\n", p.bindAddr)
	if err := http.ListenAndServe(p.bindAddr, p.manager.Get()); err != nil {
		fmt.Printf("ERROR: Manager service is failed. %s\n", err.Error())
	}
}
