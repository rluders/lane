package lane

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"
)

const (
	defaultHealthCheckPath    = "/live"
	defaultHealthCheckTimeout = 5 * time.Second
)

// HealthCheckOption configures CheckHealth and RunHealthCheck.
type HealthCheckOption func(*healthCheckConfig)

type healthCheckConfig struct {
	path    string
	timeout time.Duration
	status  int
}

// WithHealthPath sets the request path (default "/live").
func WithHealthPath(path string) HealthCheckOption {
	return func(c *healthCheckConfig) { c.path = path }
}

// WithHealthTimeout bounds the whole request (default 5s).
func WithHealthTimeout(d time.Duration) HealthCheckOption {
	return func(c *healthCheckConfig) { c.timeout = d }
}

// WithExpectedStatus sets the status code treated as healthy (default 200).
func WithExpectedStatus(code int) HealthCheckOption {
	return func(c *healthCheckConfig) { c.status = code }
}

// CheckHealth sends a GET to the health endpoint at addr and returns nil if it
// responds with the expected status. addr is ":port" (localhost is assumed) or
// "host:port".
func CheckHealth(ctx context.Context, addr string, opts ...HealthCheckOption) error {
	cfg := healthCheckConfig{path: defaultHealthCheckPath, timeout: defaultHealthCheckTimeout, status: http.StatusOK}
	for _, o := range opts {
		o(&cfg)
	}

	if strings.HasPrefix(addr, ":") {
		addr = "localhost" + addr
	}
	if !strings.HasPrefix(cfg.path, "/") {
		cfg.path = "/" + cfg.path
	}
	url := "http://" + addr + cfg.path

	ctx, cancel := context.WithTimeout(ctx, cfg.timeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return fmt.Errorf("health check %s: %w", url, err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("health check %s: %w", url, err)
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)

	if resp.StatusCode != cfg.status {
		return fmt.Errorf("health check %s: got status %d, want %d", url, resp.StatusCode, cfg.status)
	}
	return nil
}

// RunHealthCheck exits the process with 0 (healthy) or 1 (unhealthy) when
// "healthcheck" is passed as the first argument; otherwise it returns. Call at
// the top of main() before any initialization to support CMD-based container
// health probes. See CheckHealth for addr and options; on failure the reason
// is printed to stderr.
func RunHealthCheck(addr string, opts ...HealthCheckOption) {
	if len(os.Args) < 2 || os.Args[1] != "healthcheck" {
		return
	}
	if err := CheckHealth(context.Background(), addr, opts...); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	os.Exit(0)
}

// LivenessHandler returns 200 while h is live (every state but Stopped) and
// 503 otherwise. The body reports the lifecycle state.
func LivenessHandler(h *Health) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !h.Live() {
			writeHealth(w, http.StatusServiceUnavailable, "unavailable", h.State())
			return
		}
		writeHealth(w, http.StatusOK, "ok", h.State())
	}
}

// ReadinessHandler returns 200 while h is ready (Running) and all checks pass,
// 503 otherwise. Pass check functions for external dependencies (e.g.,
// db.PingContext). The body reports the lifecycle state.
func ReadinessHandler(h *Health, checks ...func(ctx context.Context) error) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		state := h.State()
		if !h.Ready() {
			writeHealth(w, http.StatusServiceUnavailable, "unavailable", state)
			return
		}
		for _, check := range checks {
			if err := check(r.Context()); err != nil {
				writeHealth(w, http.StatusServiceUnavailable, "unavailable", state)
				return
			}
		}
		writeHealth(w, http.StatusOK, "ready", state)
	}
}

func writeHealth(w http.ResponseWriter, code int, status string, state State) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(map[string]string{"status": status, "state": state.String()})
}
