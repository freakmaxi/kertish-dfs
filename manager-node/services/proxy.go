package services

import (
	"fmt"
	"net/http"

	"github.com/freakmaxi/kertish-dfs/manager-node/routing"
	"go.uber.org/zap"
)

// Proxy struct is the object for REST service execution
type Proxy struct {
	bindAddr string
	manager  *routing.Manager
	logger   *zap.Logger
}

// NewProxy creates a new instance of proxy rest service
func NewProxy(bindAddr string, manager *routing.Manager, logger *zap.Logger) *Proxy {
	return &Proxy{
		bindAddr: bindAddr,
		manager:  manager,
		logger:   logger,
	}
}

// Start starts the proxy service
func (p *Proxy) Start() {
	p.logger.Info(fmt.Sprintf("Manager Service is running on %s", p.bindAddr))
	if err := http.ListenAndServe(p.bindAddr, p.manager.Get()); err != nil {
		p.logger.Error("Manager service is failed", zap.Error(err))
	}
}
