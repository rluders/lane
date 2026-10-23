package runners

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

// ConnHandler serves one TCP connection. ctx is cancelled when shutdown
// begins; handlers should finish their current exchange and return. The
// runner closes conn after the handler returns.
type ConnHandler func(ctx context.Context, conn net.Conn)

// TCPRunner accepts TCP connections on an address and serves each one with a
// ConnHandler in its own goroutine.
//
// Lifecycle:
//   - Start binds the listener (a bind error fails startup), then accepts until
//     its ctx is cancelled. It returns only after every handler has returned.
//   - WaitReady returns once the listener is bound.
//   - When shutdown begins, the listener is closed and handlers' ctx is
//     cancelled. Stop waits for handlers to return until its ctx deadline,
//     then force-closes the remaining connections.
//   - Temporary accept errors are retried with a short backoff; other accept
//     errors make Start return the error.
//   - WithTLS serves TLS.
type TCPRunner struct {
	name     string
	addr     string
	handler  ConnHandler
	log      *slog.Logger
	certFile string
	keyFile  string
	ready    chan struct{} // closed once the listener is bound
	addrVal  atomic.Value  // net.Addr of the bound listener

	mu       sync.Mutex
	conns    map[net.Conn]struct{}
	closing  bool // set by Stop; no handlers are added afterwards
	handlers sync.WaitGroup
}

// NewTCPRunner creates a TCPRunner for addr (e.g. ":9000"). Applicable
// options: WithLogger, WithTLS.
func NewTCPRunner(name, addr string, handler ConnHandler, opts ...Option) *TCPRunner {
	o := newOptions(opts)
	return &TCPRunner{
		name:     name,
		addr:     addr,
		handler:  handler,
		log:      o.log,
		certFile: o.certFile,
		keyFile:  o.keyFile,
		ready:    make(chan struct{}),
		conns:    make(map[net.Conn]struct{}),
	}
}

func (r *TCPRunner) Name() string { return r.name }

func (r *TCPRunner) Start(ctx context.Context) error {
	r.mu.Lock()
	r.closing = false
	r.mu.Unlock()

	ln, err := r.listen()
	if err != nil {
		return fmt.Errorf("tcp runner %s: %w", r.name, err)
	}
	r.addrVal.Store(ln.Addr())
	r.log.Info("TCP server listening", "runner", r.name, "addr", ln.Addr().String())
	close(r.ready)

	stopAccept := context.AfterFunc(ctx, func() { ln.Close() })
	defer stopAccept()

	// Handlers get their own ctx so a fatal accept error can end them too
	// (Lane only cancels ctx after Start returns). Deferred calls run LIFO:
	// cancel the handlers, then wait for them.
	hctx, hcancel := context.WithCancel(ctx)
	defer r.handlers.Wait()
	defer hcancel()

	var delay time.Duration
	for {
		conn, err := ln.Accept()
		if err != nil {
			if ctx.Err() != nil || errors.Is(err, net.ErrClosed) {
				return nil
			}
			var ne net.Error
			if errors.As(err, &ne) && ne.Timeout() {
				delay = min(max(2*delay, 5*time.Millisecond), time.Second)
				r.log.Warn("tcp accept error; retrying", "runner", r.name, "error", err, "delay", delay)
				select {
				case <-time.After(delay):
					continue
				case <-ctx.Done():
					return nil
				}
			}
			ln.Close()
			return fmt.Errorf("tcp runner %s: accept: %w", r.name, err)
		}
		delay = 0
		r.serve(hctx, conn)
	}
}

func (r *TCPRunner) listen() (net.Listener, error) {
	ln, err := net.Listen("tcp", r.addr)
	if err != nil || r.certFile == "" {
		return ln, err
	}
	cert, err := tls.LoadX509KeyPair(r.certFile, r.keyFile)
	if err != nil {
		ln.Close()
		return nil, err
	}
	return tls.NewListener(ln, &tls.Config{Certificates: []tls.Certificate{cert}}), nil
}

func (r *TCPRunner) serve(ctx context.Context, conn net.Conn) {
	r.mu.Lock()
	if r.closing {
		r.mu.Unlock()
		conn.Close()
		return
	}
	r.conns[conn] = struct{}{}
	r.handlers.Add(1) // under mu, so it can't race with Stop's Wait
	r.mu.Unlock()
	go func() {
		defer r.handlers.Done()
		defer func() {
			r.mu.Lock()
			delete(r.conns, conn)
			r.mu.Unlock()
			conn.Close()
		}()
		r.handler(ctx, conn)
	}()
}

// Addr returns the bound listener address, or nil before the listener is
// bound. Useful with port 0.
func (r *TCPRunner) Addr() net.Addr {
	a, _ := r.addrVal.Load().(net.Addr)
	return a
}

// WaitReady blocks until the listener is bound or ctx is done.
func (r *TCPRunner) WaitReady(ctx context.Context) error {
	select {
	case <-r.ready:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Stop waits for active handlers to return until ctx is done, then closes the
// remaining connections and returns ctx.Err().
func (r *TCPRunner) Stop(ctx context.Context) error {
	r.mu.Lock()
	r.closing = true
	r.mu.Unlock()

	done := make(chan struct{})
	go func() { r.handlers.Wait(); close(done) }()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		r.mu.Lock()
		for c := range r.conns {
			c.Close()
		}
		r.mu.Unlock()
		return ctx.Err()
	}
}
