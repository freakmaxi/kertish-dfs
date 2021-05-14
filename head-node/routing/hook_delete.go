package routing

import (
	"encoding/json"
	"net/http"
	"os"

	"go.uber.org/zap"
)

func (h *hookRouter) handleDelete(w http.ResponseWriter, r *http.Request) {
	requestedPaths, err := h.describeXPath(r.Header.Get("X-Path"))
	if err != nil || len(requestedPaths) > 1 {
		w.WriteHeader(422)
		return
	}

	hookIds := make([]string, 0)
	if err := json.NewDecoder(r.Body).Decode(&hookIds); err != nil {
		w.WriteHeader(422)
		return
	}

	if err := h.hook.Delete(requestedPaths[0], hookIds); err != nil {
		if err == os.ErrNotExist {
			w.WriteHeader(404)
			return
		} else {
			w.WriteHeader(500)
		}
		h.logger.Error("Delete hook request is failed", zap.String("path", requestedPaths[0]), zap.Error(err))
	}
}
