package runners

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
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
}

// NewHTTPRunner creates a plain HTTP runner.
func NewHTTPRunner(name string, server *http.Server, log *slog.Logger) *HTTPRunner {
	return &HTTPRunner{name: name, server: server, log: log}
}

// NewHTTPSRunner creates a TLS HTTP runner.
func NewHTTPSRunner(name string, server *http.Server, certFile, keyFile string, log *slog.Logger) *HTTPRunner {
	return &HTTPRunner{name: name, server: server, certFile: certFile, keyFile: keyFile, log: log}
}

func (r *HTTPRunner) Name() string { return r.name }

func (r *HTTPRunner) Start(ctx context.Context) error {
	r.log.Info("HTTP server listening", "runner", r.name, "addr", r.server.Addr)

	var err error
	if r.certFile != "" {
		err = r.server.ListenAndServeTLS(r.certFile, r.keyFile)
	} else {
		err = r.server.ListenAndServe()
	}

	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return fmt.Errorf("http runner %s: %w", r.name, err)
}

func (r *HTTPRunner) Stop(ctx context.Context) error {
	return r.server.Shutdown(ctx)
}
