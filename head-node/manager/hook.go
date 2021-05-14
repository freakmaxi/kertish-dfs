package manager

import (
	"github.com/freakmaxi/kertish-dfs/head-node/data"
	"github.com/freakmaxi/kertish-dfs/hooks"
	"go.uber.org/zap"
)

// Hook interface is for hook manipulation operations base on REST service request
type Hook interface {
	GetAvailableList() []interface{}

	Add(folderPaths []string, hook *hooks.Hook) error
	Delete(folderPath string, hookIds []string) error
}

type hook struct {
	metadata data.Metadata
	logger   *zap.Logger
}

// NewHook creates the instance of hook manipulation operations object for REST service request
func NewHook(metadata data.Metadata, logger *zap.Logger) Hook {
	return &hook{
		metadata: metadata,
		logger:   logger,
	}
}

func (h *hook) GetAvailableList() []interface{} {
	availableHooks := make([]interface{}, 0)

	actions := hooks.CurrentLoader.List()
	for _, action := range actions {
		availableHooks = append(availableHooks, &struct {
			Provider string      `json:"provider"`
			Sample   interface{} `json:"sample"`
		}{
			Provider: action.Provider(),
			Sample:   action.Sample(),
		})
	}

	return availableHooks
}

var _ Hook = &hook{}
