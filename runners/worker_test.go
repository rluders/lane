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

func TestWithBackoff(t *testing.T) {
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	noop := func(context.Context) error { return nil }

	tests := []struct {
		name         string
		initial, max time.Duration
		wantBase     time.Duration
		wantMax      time.Duration
	}{
		{"custom", 50 * time.Millisecond, 5 * time.Second, 50 * time.Millisecond, 5 * time.Second},
		{"zero keeps defaults", 0, 0, defaultWorkerBaseBackoff, defaultWorkerMaxBackoff},
		{"max below initial is raised", time.Minute, time.Second, time.Minute, time.Minute},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := NewWorkerRunner("w", noop, log, WithBackoff(tt.initial, tt.max))
			if w.baseBackoff != tt.wantBase || w.maxBackoff != tt.wantMax {
				t.Fatalf("got base=%v max=%v, want base=%v max=%v", w.baseBackoff, w.maxBackoff, tt.wantBase, tt.wantMax)
			}
		})
	}
}

func TestWithBackoff_AppliesToStart(t *testing.T) {
	var calls atomic.Int64
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	w := NewWorkerRunner("w", func(ctx context.Context) error {
		calls.Add(1)
		return errWork
	}, log, WithBackoff(time.Hour, time.Hour))

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	if err := w.Start(ctx); err != nil {
		t.Fatalf("Start returned %v", err)
	}
	if n := calls.Load(); n != 1 {
		t.Fatalf("expected 1 call within the configured backoff, got %d", n)
	}
}

func TestBackoffJitter(t *testing.T) {
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	noop := func(context.Context) error { return nil }

	plain := NewWorkerRunner("w", noop, log)
	if d := plain.delay(time.Second); d != time.Second {
		t.Fatalf("without jitter: got %v, want 1s", d)
	}

	jittered := NewWorkerRunner("w", noop, log, WithBackoffJitter())
	seen := map[time.Duration]bool{}
	for range 100 {
		d := jittered.delay(time.Second)
		if d < 500*time.Millisecond || d > time.Second {
			t.Fatalf("jittered delay %v outside [500ms, 1s]", d)
		}
		seen[d] = true
	}
	if len(seen) < 2 {
		t.Fatal("jitter produced identical delays")
	}
}
