package lane

import (
	"context"
	"encoding/json"
	"net/http"
	"os"
	"sync"
)

// HealthState tracks the application's readiness.
// Liveness is always true — if the process is running, it's alive.
type HealthState struct {
	mu    sync.RWMutex
	ready bool
}

func (h *HealthState) SetReady(v bool) {
	h.mu.Lock()
	h.ready = v
	h.mu.Unlock()
}

func (h *HealthState) IsReady() bool {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.ready
}

// RunHealthCheck exits the process with 0 (healthy) or 1 (unhealthy) when
// "healthcheck" is passed as the first argument. Call at the top of main()
// before any initialization to support CMD-based container health probes.
func RunHealthCheck(addr string) {
	if len(os.Args) < 2 || os.Args[1] != "healthcheck" {
		return
	}
	resp, err := http.Get("http://localhost" + addr + "/health") //nolint:noctx
	if err != nil || resp.StatusCode != http.StatusOK {
		os.Exit(1)
	}
	os.Exit(0)
}

// LivenessHandler always returns 200. A running process is a live process.
func LivenessHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
	}
}

// ReadinessHandler returns 200 when the app is ready and all checks pass, 503 otherwise.
// Pass check functions for external dependencies (e.g., db.PingContext).
func ReadinessHandler(state *HealthState, checks ...func(ctx context.Context) error) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		if !state.IsReady() {
			w.WriteHeader(http.StatusServiceUnavailable)
			_ = json.NewEncoder(w).Encode(map[string]string{"status": "unavailable"})
			return
		}

		for _, check := range checks {
			if err := check(r.Context()); err != nil {
				w.WriteHeader(http.StatusServiceUnavailable)
				_ = json.NewEncoder(w).Encode(map[string]string{"status": "unavailable"})
				return
			}
		}

		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(map[string]string{"status": "ready"})
	}
}
