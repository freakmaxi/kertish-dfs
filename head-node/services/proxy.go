package services

import (
	"fmt"
	"net/http"

	"github.com/freakmaxi/kertish-dfs/head-node/routing"
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
	fmt.Printf("INFO: Head Service is running on %s\n", p.bindAddr)
	if err := http.ListenAndServe(p.bindAddr, p.manager.Get()); err != nil {
		fmt.Printf("ERROR: Head service is failed. %s\n", err.Error())
	}
}
