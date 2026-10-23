package runners

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

var errWork = errors.New("work failed")

func newTestWorker(fn WorkFn, base, max time.Duration, opts ...WorkerOption) *WorkerRunner {
	w := NewWorkerRunner("test", fn, slog.New(slog.NewTextHandler(io.Discard, nil)), opts...)
	w.baseBackoff = base
	w.maxBackoff = max
	return w
}

func TestWorker_RepeatedFailuresAreBounded(t *testing.T) {
	var calls atomic.Int64
	w := newTestWorker(func(ctx context.Context) error {
		calls.Add(1)
		return errWork
	}, 10*time.Millisecond, 40*time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	if err := w.Start(ctx); err != nil {
		t.Fatalf("Start returned %v", err)
	}

	// 10+20+40+40+40... → at most ~7 calls in 200ms.
	if n := calls.Load(); n > 10 {
		t.Fatalf("expected bounded calls, got %d", n)
	}
}

func TestWorker_BackoffGrowsAndCaps(t *testing.T) {
	var mu sync.Mutex
	var times []time.Time
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	w := newTestWorker(func(ctx context.Context) error {
		mu.Lock()
		times = append(times, time.Now())
		n := len(times)
		mu.Unlock()
		if n == 6 {
			cancel()
		}
		return errWork
	}, 10*time.Millisecond, 40*time.Millisecond)

	if err := w.Start(ctx); err != nil {
		t.Fatalf("Start returned %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	want := []time.Duration{10, 20, 40, 40, 40}
	for i, d := range want {
		gap := times[i+1].Sub(times[i])
		min := d * time.Millisecond
		if gap < min || gap > min+50*time.Millisecond {
			t.Errorf("gap %d = %v, want ~%v", i, gap, min)
		}
	}
}

func TestWorker_CancellationDuringBackoff(t *testing.T) {
	failed := make(chan struct{}, 1)
	w := newTestWorker(func(ctx context.Context) error {
		select {
		case failed <- struct{}{}:
		default:
		}
		return errWork
	}, 10*time.Second, 10*time.Second)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- w.Start(ctx) }()

	<-failed
	start := time.Now()
	cancel()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Start returned %v", err)
		}
		if d := time.Since(start); d > 50*time.Millisecond {
			t.Fatalf("Start took %v to return after cancel", d)
		}
	case <-time.After(time.Second):
		t.Fatal("Start did not return after cancel")
	}
}

func TestWorker_SuccessResetsBackoff(t *testing.T) {
	// Fail 3 times (backoff reaches 80ms), succeed once, then fail again:
	// the delay after the next failure must be back to base.
	var mu sync.Mutex
	var times []time.Time
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	w := newTestWorker(func(ctx context.Context) error {
		mu.Lock()
		times = append(times, time.Now())
		n := len(times)
		mu.Unlock()
		switch {
		case n == 4:
			return nil
		case n == 6:
			cancel()
		}
		return errWork
	}, 10*time.Millisecond, time.Second)

	if err := w.Start(ctx); err != nil {
		t.Fatalf("Start returned %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	// times[4] is the failure right after the success; times[5] follows its backoff.
	if gap := times[5].Sub(times[4]); gap > 40*time.Millisecond {
		t.Fatalf("backoff not reset after success: gap %v", gap)
	}
}

func TestWorker_NoErrorReportedOnShutdown(t *testing.T) {
	var reported atomic.Int64
	ctx, cancel := context.WithCancel(context.Background())

	w := newTestWorker(func(ctx context.Context) error {
		cancel()
		return ctx.Err()
	}, 10*time.Millisecond, 10*time.Millisecond, WithErrorHandler(func(error) {
		reported.Add(1)
	}))

	if err := w.Start(ctx); err != nil {
		t.Fatalf("Start returned %v", err)
	}
	if n := reported.Load(); n != 0 {
		t.Fatalf("onError called %d times on shutdown", n)
	}
}
