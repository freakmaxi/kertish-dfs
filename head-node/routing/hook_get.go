package routing

import (
	"encoding/json"
	"net/http"

	"go.uber.org/zap"
)

func (h *hookRouter) handleGet(w http.ResponseWriter, _ *http.Request) {
	availableHooks := h.hook.GetAvailableList()

	if err := json.NewEncoder(w).Encode(availableHooks); err != nil {
		w.WriteHeader(500)
		h.logger.Error(
			"Response of available hooks list request is failed",
			zap.Error(err),
		)
	}
}
