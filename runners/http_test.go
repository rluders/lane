package runners

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"testing"
	"time"
)

func newTestHTTPRunner(addr string) *HTTPRunner {
	srv := &http.Server{Addr: addr, Handler: http.NotFoundHandler()}
	return NewHTTPRunner("http", srv, WithLogger(slog.New(slog.NewTextHandler(io.Discard, nil))))
}

func TestHTTPRunner_ReadyAfterListen(t *testing.T) {
	r := newTestHTTPRunner("127.0.0.1:0")

	// Not ready before Start.
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := r.WaitReady(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Ready before Start: got %v, want deadline exceeded", err)
	}

	done := make(chan error, 1)
	go func() { done <- r.Start(context.Background()) }()

	readyCtx, readyCancel := context.WithTimeout(context.Background(), time.Second)
	defer readyCancel()
	if err := r.WaitReady(readyCtx); err != nil {
		t.Fatalf("Ready: %v", err)
	}

	if err := r.Stop(context.Background()); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	if err := <-done; err != nil {
		t.Fatalf("Start: %v", err)
	}
}

func TestHTTPRunner_ListenFailureNeverReady(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	r := newTestHTTPRunner(ln.Addr().String()) // port already in use
	if err := r.Start(context.Background()); err == nil {
		t.Fatal("expected listen error")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := r.WaitReady(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Ready after failed Start: got %v, want deadline exceeded", err)
	}
}
