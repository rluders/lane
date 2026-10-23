package runners

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
)

// HTTPRunner wraps an *http.Server as a Runner. The caller configures the
// server (timeouts, handler, TLS config); use WithTLS to serve TLS from
// certificate and key files.
type HTTPRunner struct {
	name     string
	server   *http.Server
	log      *slog.Logger
	certFile string
	keyFile  string
	ready    chan struct{} // closed once the listener is bound
}

// NewHTTPRunner creates an HTTP runner. Applicable options: WithLogger,
// WithTLS.
func NewHTTPRunner(name string, server *http.Server, opts ...Option) *HTTPRunner {
	o := newOptions(opts)
	return &HTTPRunner{
		name:     name,
		server:   server,
		log:      o.log,
		certFile: o.certFile,
		keyFile:  o.keyFile,
		ready:    make(chan struct{}),
	}
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

// WaitReady blocks until the listener is bound or ctx is done.
func (r *HTTPRunner) WaitReady(ctx context.Context) error {
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
