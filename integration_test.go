package lane_test

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/rluders/lane/v2"
	"github.com/rluders/lane/v2/runners"
)

func freeAddr(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	return ln.Addr().String()
}

func eventually(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// newApp wires a realistic service: HTTP API with health endpoints, a worker
// and a scheduler. It returns the lane, the API address, and work counters.
func newApp(t *testing.T, workerErr error) (*lane.Lane, string, *atomic.Int64, *atomic.Int64) {
	t.Helper()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	l := lane.New(lane.WithLogger(log), lane.WithShutdownTimeout(2*time.Second))

	addr := freeAddr(t)
	mux := http.NewServeMux()
	mux.Handle("GET /live", lane.LivenessHandler(l.Health()))
	mux.Handle("GET /ready", lane.ReadinessHandler(l.Health()))
	srv := &http.Server{Addr: addr, Handler: lane.RecoverMiddleware(log)(mux)}

	var work, jobs atomic.Int64
	worker := runners.NewWorkerRunner("worker", func(ctx context.Context) error {
		work.Add(1)
		if workerErr != nil {
			return workerErr
		}
		select {
		case <-ctx.Done():
		case <-time.After(5 * time.Millisecond):
		}
		return nil
	}, runners.WithLogger(log), runners.WithBackoff(5*time.Millisecond, 20*time.Millisecond))
	sched := runners.NewSchedulerRunner("sched", 10*time.Millisecond, func(context.Context) error {
		jobs.Add(1)
		return nil
	}, runners.WithLogger(log), runners.WithImmediateRun())

	l.AddRunner(runners.NewHTTPRunner("api", srv, runners.WithLogger(log)), worker, sched)
	return l, addr, &work, &jobs
}

func TestIntegration_FullLifecycle(t *testing.T) {
	l, addr, work, jobs := newApp(t, nil)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- l.Run(ctx) }()

	eventually(t, "ready", func() bool {
		return lane.CheckHealth(context.Background(), addr, lane.WithHealthPath("/ready")) == nil
	})
	if err := lane.CheckHealth(context.Background(), addr); err != nil {
		t.Fatalf("liveness: %v", err)
	}
	eventually(t, "work and jobs", func() bool { return work.Load() > 2 && jobs.Load() > 2 })

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return")
	}
	if s := l.Health().State(); s != lane.StateStopped {
		t.Fatalf("final state %v", s)
	}
	if err := lane.CheckHealth(context.Background(), addr, lane.WithHealthTimeout(200*time.Millisecond)); err == nil {
		t.Fatal("HTTP server still serving after shutdown")
	}
}

func TestIntegration_SIGTERM(t *testing.T) {
	l, addr, _, _ := newApp(t, nil)
	done := make(chan error, 1)
	go func() { done <- l.Run(context.Background()) }()

	eventually(t, "ready", func() bool {
		return lane.CheckHealth(context.Background(), addr, lane.WithHealthPath("/ready")) == nil
	})
	if err := syscall.Kill(syscall.Getpid(), syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run after SIGTERM: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after SIGTERM")
	}
}

func TestIntegration_FailingWorkerKeepsServiceUp(t *testing.T) {
	l, addr, work, _ := newApp(t, errors.New("queue unavailable"))
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- l.Run(ctx) }()

	eventually(t, "ready", func() bool {
		return lane.CheckHealth(context.Background(), addr, lane.WithHealthPath("/ready")) == nil
	})
	time.Sleep(100 * time.Millisecond)
	// Backoff caps the retry rate: 100ms / (5..20ms) is far below a tight loop.
	if n := work.Load(); n > 15 {
		t.Fatalf("worker retried %d times in 100ms; backoff not applied", n)
	}
	if err := lane.CheckHealth(context.Background(), addr, lane.WithHealthPath("/ready")); err != nil {
		t.Fatalf("worker errors should not affect readiness: %v", err)
	}

	cancel()
	if err := <-done; err != nil {
		t.Fatalf("Run: %v", err)
	}
}

func TestIntegration_PortInUseFailsStartup(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	l := lane.New(lane.WithLogger(log), lane.WithShutdownTimeout(time.Second))
	srv := &http.Server{Addr: ln.Addr().String()}
	worker := runners.NewWorkerRunner("worker", func(ctx context.Context) error {
		<-ctx.Done()
		return nil
	}, runners.WithLogger(log))
	l.AddRunner(runners.NewHTTPRunner("api", srv, runners.WithLogger(log)), worker)

	done := make(chan error, 1)
	go func() { done <- l.Run(context.Background()) }()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("expected bind error")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after startup failure")
	}
	if l.Health().State() != lane.StateStopped {
		t.Fatalf("final state %v", l.Health().State())
	}
}

func TestIntegration_SupervisorKeepsLaneUp(t *testing.T) {
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	l := lane.New(lane.WithLogger(log), lane.WithShutdownTimeout(time.Second))

	var attempts atomic.Int64
	flaky := runners.NewFuncRunner("flaky", func(ctx context.Context) error {
		if attempts.Add(1) <= 3 {
			return errors.New("transient")
		}
		<-ctx.Done()
		return nil
	})
	l.AddRunner(runners.NewSupervisor(flaky, runners.WithLogger(log),
		runners.WithBackoff(time.Millisecond, 5*time.Millisecond)))

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- l.Run(ctx) }()
	eventually(t, "recovered after restarts", func() bool { return attempts.Load() >= 4 })
	if s := l.Health().State(); s != lane.StateRunning {
		t.Fatalf("transient failures should not stop the lane: %v", s)
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("Run: %v", err)
	}
}
