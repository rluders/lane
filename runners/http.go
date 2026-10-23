package runners

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
)

// HTTPRunner wraps an *http.Server as a Runner.
// The caller is responsible for configuring the server (timeouts, handler, TLS).
type HTTPRunner struct {
	name     string
	server   *http.Server
	log      *slog.Logger
	certFile string
	keyFile  string
	ready    chan struct{} // closed once the listener is bound
}

// NewHTTPRunner creates a plain HTTP runner.
func NewHTTPRunner(name string, server *http.Server, log *slog.Logger) *HTTPRunner {
	return &HTTPRunner{name: name, server: server, log: log, ready: make(chan struct{})}
}

// NewHTTPSRunner creates a TLS HTTP runner.
func NewHTTPSRunner(name string, server *http.Server, certFile, keyFile string, log *slog.Logger) *HTTPRunner {
	return &HTTPRunner{name: name, server: server, certFile: certFile, keyFile: keyFile, log: log, ready: make(chan struct{})}
}

func (r *HTTPRunner) Name() string { return r.name }

func (r *HTTPRunner) Start(ctx context.Context) error {
	addr := r.server.Addr
	if addr == "" {
		addr = ":http"
		if r.certFile != "" {
			addr = ":https"
		}
	}

	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("http runner %s: %w", r.name, err)
	}
	r.log.Info("HTTP server listening", "runner", r.name, "addr", ln.Addr().String())
	close(r.ready)

	if r.certFile != "" {
		err = r.server.ServeTLS(ln, r.certFile, r.keyFile)
	} else {
		err = r.server.Serve(ln)
	}

	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return fmt.Errorf("http runner %s: %w", r.name, err)
}

// Ready blocks until the listener is bound or ctx is done.
func (r *HTTPRunner) Ready(ctx context.Context) error {
	select {
	case <-r.ready:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (r *HTTPRunner) Stop(ctx context.Context) error {
	return r.server.Shutdown(ctx)
}
