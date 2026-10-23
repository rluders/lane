package runners

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

var errAttempt = errors.New("attempt failed")

// scripted returns a Restartable whose Start runs script(attempt) for each
// 0-based attempt.
func scripted(script func(ctx context.Context, attempt int) error) (*FuncRunner, *atomic.Int32) {
	var n atomic.Int32
	return NewFuncRunner("svc", func(ctx context.Context) error {
		return script(ctx, int(n.Add(1))-1)
	}), &n
}

func fastSupervisor(r Restartable, opts ...Option) *Supervisor {
	return NewSupervisor(r, append([]Option{quietLog(), WithBackoff(time.Millisecond, 4*time.Millisecond)}, opts...)...)
}

func TestSupervisor_RestartsUntilSuccess(t *testing.T) {
	var reported atomic.Int32
	r, attempts := scripted(func(ctx context.Context, i int) error {
		if i < 3 {
			return errAttempt
		}
		return nil // finished
	})
	s := fastSupervisor(r, WithErrorHandler(func(error) { reported.Add(1) }))
	if err := s.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if attempts.Load() != 4 || reported.Load() != 3 {
		t.Fatalf("attempts=%d reported=%d", attempts.Load(), reported.Load())
	}
}

func TestSupervisor_GivesUpAfterMaxRestarts(t *testing.T) {
	r, attempts := scripted(func(context.Context, int) error { return errAttempt })
	s := fastSupervisor(r, WithMaxRestarts(2))
	err := s.Start(context.Background())
	if !errors.Is(err, errAttempt) {
		t.Fatalf("expected last error, got %v", err)
	}
	if attempts.Load() != 3 {
		t.Fatalf("expected 1 attempt + 2 restarts, got %d attempts", attempts.Load())
	}
}

func TestSupervisor_RestartsAfterPanic(t *testing.T) {
	r, attempts := scripted(func(ctx context.Context, i int) error {
		if i == 0 {
			panic("boom")
		}
		return nil
	})
	if err := fastSupervisor(r).Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if attempts.Load() != 2 {
		t.Fatalf("attempts=%d", attempts.Load())
	}
}

func TestSupervisor_CancellationDuringBackoff(t *testing.T) {
	started := make(chan struct{}, 1)
	r, _ := scripted(func(context.Context, int) error {
		select {
		case started <- struct{}{}:
		default:
		}
		return errAttempt
	})
	s := NewSupervisor(r, quietLog(), WithBackoff(10*time.Second, 10*time.Second))
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- s.Start(ctx) }()
	<-started
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Start: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("backoff not interrupted by cancellation")
	}
}

func TestSupervisor_NoRestartOnShutdown(t *testing.T) {
	r, attempts := scripted(func(ctx context.Context, _ int) error {
		<-ctx.Done()
		return ctx.Err()
	})
	ctx, cancel := context.WithCancel(context.Background())
	time.AfterFunc(10*time.Millisecond, cancel)
	if err := fastSupervisor(r).Start(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("Start should pass the ctx error through: %v", err)
	}
	if attempts.Load() != 1 {
		t.Fatalf("restarted during shutdown: %d attempts", attempts.Load())
	}
}

func TestSupervisor_HealthyRunResetsRestarts(t *testing.T) {
	// Every attempt runs longer than the backoff cap (4ms) before failing,
	// so the restart count keeps resetting and max 1 restart is never hit.
	r, attempts := scripted(func(ctx context.Context, i int) error {
		if i == 5 {
			return nil
		}
		time.Sleep(10 * time.Millisecond)
		return errAttempt
	})
	if err := fastSupervisor(r, WithMaxRestarts(1)).Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if attempts.Load() != 6 {
		t.Fatalf("attempts=%d", attempts.Load())
	}
}

type readyStub struct {
	*FuncRunner
	ready chan struct{}
}

func (r readyStub) WaitReady(ctx context.Context) error {
	select {
	case <-r.ready:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func TestSupervisor_ForwardsNameReadyStop(t *testing.T) {
	var stopped atomic.Bool
	inner := readyStub{
		FuncRunner: NewFuncRunner("db", func(context.Context) error { return nil },
			WithStop(func(context.Context) error { stopped.Store(true); return nil })),
		ready: make(chan struct{}),
	}
	s := NewSupervisor(inner)
	if s.Name() != "db" {
		t.Fatalf("name %q", s.Name())
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := s.WaitReady(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("WaitReady should forward: %v", err)
	}
	close(inner.ready)
	if err := s.WaitReady(context.Background()); err != nil {
		t.Fatalf("WaitReady: %v", err)
	}
	if err := s.Stop(context.Background()); err != nil || !stopped.Load() {
		t.Fatalf("Stop should forward: %v", err)
	}
	if err := NewSupervisor(NewFuncRunner("x", nil)).WaitReady(context.Background()); err != nil {
		t.Fatalf("WaitReady without Readier: %v", err)
	}
}
