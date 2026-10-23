package runners

import (
	"bufio"
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"sync/atomic"
	"testing"
	"time"
)

func quietLog() Option { return WithLogger(slog.New(slog.NewTextHandler(io.Discard, nil))) }

// startTCP starts r and waits until it is ready.
func startTCP(t *testing.T, r *TCPRunner) (context.CancelFunc, <-chan error) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- r.Start(ctx) }()
	rctx, rcancel := context.WithTimeout(context.Background(), time.Second)
	defer rcancel()
	if err := r.WaitReady(rctx); err != nil {
		t.Fatalf("WaitReady: %v", err)
	}
	return cancel, done
}

func stopCtx(t *testing.T, d time.Duration) context.Context {
	ctx, cancel := context.WithTimeout(context.Background(), d)
	t.Cleanup(cancel)
	return ctx
}

func TestTCPRunner_ServesConnections(t *testing.T) {
	r := NewTCPRunner("echo", "127.0.0.1:0", func(ctx context.Context, conn net.Conn) {
		_, _ = io.Copy(conn, conn)
	}, quietLog())
	if r.Addr() != nil {
		t.Fatal("Addr should be nil before Start")
	}
	cancel, done := startTCP(t, r)

	for range 3 { // several concurrent clients
		conn, err := net.Dial("tcp", r.Addr().String())
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Close()
		_, _ = conn.Write([]byte("ping\n"))
		line, err := bufio.NewReader(conn).ReadString('\n')
		if err != nil || line != "ping\n" {
			t.Fatalf("echo: %q %v", line, err)
		}
	}

	cancel()
	// echo handlers block in io.Copy until the client closes or Stop
	// force-closes; Stop with a short deadline closes them.
	if err := r.Stop(stopCtx(t, 50*time.Millisecond)); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Stop should force-close lingering connections: %v", err)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Start: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Start did not return after handlers were closed")
	}
}

func TestTCPRunner_GracefulHandlersFinish(t *testing.T) {
	var finished atomic.Bool
	r := NewTCPRunner("graceful", "127.0.0.1:0", func(ctx context.Context, conn net.Conn) {
		<-ctx.Done() // shutdown signal reaches the handler
		time.Sleep(20 * time.Millisecond)
		finished.Store(true)
	}, quietLog())
	cancel, done := startTCP(t, r)

	conn, err := net.Dial("tcp", r.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	time.Sleep(20 * time.Millisecond) // let the handler start

	cancel()
	if err := r.Stop(stopCtx(t, time.Second)); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	if !finished.Load() {
		t.Fatal("Stop returned before the handler finished")
	}
	if err := <-done; err != nil {
		t.Fatalf("Start: %v", err)
	}
	if _, err := net.DialTimeout("tcp", r.Addr().String(), 100*time.Millisecond); err == nil {
		t.Fatal("listener still accepting after shutdown")
	}
}

func TestTCPRunner_BindFailure(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	r := NewTCPRunner("busy", ln.Addr().String(), func(context.Context, net.Conn) {}, quietLog())
	if err := r.Start(context.Background()); err == nil {
		t.Fatal("expected bind error")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := r.WaitReady(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("WaitReady after bind failure: %v", err)
	}
	if err := r.Stop(stopCtx(t, time.Second)); err != nil {
		t.Fatalf("Stop without Start: %v", err)
	}
}

func TestTCPRunner_TLSMissingCertFailsStart(t *testing.T) {
	r := NewTCPRunner("tls", "127.0.0.1:0", func(context.Context, net.Conn) {}, quietLog(),
		WithTLS("/nonexistent/cert.pem", "/nonexistent/key.pem"))
	if err := r.Start(context.Background()); err == nil {
		t.Fatal("expected TLS load error")
	}
}

func TestFuncRunner(t *testing.T) {
	var stopped atomic.Bool
	r := NewFuncRunner("fn", func(ctx context.Context) error {
		<-ctx.Done()
		return nil
	}, WithStop(func(context.Context) error { stopped.Store(true); return nil }))

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- r.Start(ctx) }()
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("Start: %v", err)
	}
	if err := r.Stop(context.Background()); err != nil || !stopped.Load() {
		t.Fatalf("Stop: %v stopped=%v", err, stopped.Load())
	}
	if r.Name() != "fn" {
		t.Fatal("name")
	}

	errBoom := errors.New("boom")
	plain := NewFuncRunner("plain", func(context.Context) error { return errBoom })
	if err := plain.Start(context.Background()); !errors.Is(err, errBoom) {
		t.Fatalf("Start should return fn's error: %v", err)
	}
	if err := plain.Stop(context.Background()); err != nil {
		t.Fatalf("Stop without WithStop: %v", err)
	}
}
