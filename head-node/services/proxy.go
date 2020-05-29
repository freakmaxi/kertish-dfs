package services

import (
	"net/http"

	"github.com/freakmaxi/kertish-dfs/head-node/routing"
	"go.uber.org/zap"
)

type Proxy struct {
	bindAddr string
	manager  *routing.Manager
	logger   *zap.Logger
}

func NewProxy(bindAddr string, manager *routing.Manager, logger *zap.Logger) *Proxy {
	return &Proxy{
		bindAddr: bindAddr,
		manager:  manager,
		logger:   logger,
	}
}

func (p *Proxy) Start() {
	p.logger.Sugar().Infof("Head Service is running on %s", p.bindAddr)
	if err := http.ListenAndServe(p.bindAddr, p.manager.Get()); err != nil {
		p.logger.Error("Head service is failed", zap.Error(err))
	}
}
