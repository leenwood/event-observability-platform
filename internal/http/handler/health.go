package handler

import (
	"context"
	"net/http"
	"time"
)

type Pinger interface {
	Ping(ctx context.Context) error
}

type HealthHandler struct {
	db Pinger
}

func NewHealthHandler(db Pinger) *HealthHandler {
	return &HealthHandler{db: db}
}

// Health returns service liveness.
//
// @Summary  Liveness probe
// @Tags     health
// @Produce  json
// @Success  200  {object}  map[string]string  "ok"
// @Router   /health [get]
func (h *HealthHandler) Health(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// Ready returns service readiness by checking database connectivity.
//
// @Summary  Readiness probe
// @Tags     health
// @Produce  json
// @Success  200  {object}  map[string]string  "ready"
// @Failure  503  {object}  map[string]string  "database unreachable"
// @Router   /ready [get]
func (h *HealthHandler) Ready(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
	defer cancel()

	if err := h.db.Ping(ctx); err != nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{
			"status": "unavailable",
			"reason": "database unreachable",
		})
		return
	}

	writeJSON(w, http.StatusOK, map[string]string{"status": "ready"})
}

