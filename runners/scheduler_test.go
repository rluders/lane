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

func newTestScheduler(interval time.Duration, fn Func, opts ...Option) *SchedulerRunner {
	opts = append([]Option{WithLogger(slog.New(slog.NewTextHandler(io.Discard, nil)))}, opts...)
	return NewSchedulerRunner("sched", interval, fn, opts...)
}

// runFor runs s until d elapses and fails the test if Start errors.
func runFor(t *testing.T, s *SchedulerRunner, d time.Duration) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), d)
	defer cancel()
	if err := s.Start(ctx); err != nil {
		t.Fatalf("Start returned %v", err)
	}
}

func TestScheduler_DefaultWaitsForFirstInterval(t *testing.T) {
	var calls atomic.Int64
	s := newTestScheduler(time.Hour, func(context.Context) error {
		calls.Add(1)
		return nil
	})
	runFor(t, s, 50*time.Millisecond)
	if n := calls.Load(); n != 0 {
		t.Fatalf("expected no run before first interval, got %d", n)
	}
}

func TestScheduler_RunsEveryInterval(t *testing.T) {
	var calls atomic.Int64
	s := newTestScheduler(20*time.Millisecond, func(context.Context) error {
		calls.Add(1)
		return nil
	})
	runFor(t, s, 110*time.Millisecond)
	if n := calls.Load(); n < 3 || n > 6 {
		t.Fatalf("expected ~5 runs, got %d", n)
	}
}

func TestScheduler_ImmediateRun(t *testing.T) {
	var calls atomic.Int64
	s := newTestScheduler(time.Hour, func(context.Context) error {
		calls.Add(1)
		return nil
	}, WithImmediateRun())
	runFor(t, s, 50*time.Millisecond)
	if n := calls.Load(); n != 1 {
		t.Fatalf("expected exactly 1 immediate run, got %d", n)
	}
}

func TestScheduler_ImmediateRunThenSchedule(t *testing.T) {
	var mu sync.Mutex
	var times []time.Time
	start := time.Now()
	s := newTestScheduler(40*time.Millisecond, func(context.Context) error {
		mu.Lock()
		times = append(times, time.Now())
		mu.Unlock()
		return nil
	}, WithImmediateRun())
	runFor(t, s, 100*time.Millisecond)

	mu.Lock()
	defer mu.Unlock()
	if len(times) < 2 {
		t.Fatalf("expected immediate run plus scheduled runs, got %d", len(times))
	}
	if d := times[0].Sub(start); d > 20*time.Millisecond {
		t.Fatalf("immediate run took %v", d)
	}
	if d := times[1].Sub(start); d < 35*time.Millisecond || d > 70*time.Millisecond {
		t.Fatalf("first scheduled run at %v, want ~40ms after Start", d)
	}
}

func TestScheduler_CancelledBeforeImmediateRun(t *testing.T) {
	var calls atomic.Int64
	s := newTestScheduler(time.Hour, func(context.Context) error {
		calls.Add(1)
		return nil
	}, WithImmediateRun())

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := s.Start(ctx); err != nil {
		t.Fatalf("Start returned %v", err)
	}
	if n := calls.Load(); n != 0 {
		t.Fatalf("job ran after cancellation: %d", n)
	}
}

func TestScheduler_SlowJobNoOverlapAndDropsMissedTicks(t *testing.T) {
	var running, overlaps atomic.Int64
	var mu sync.Mutex
	var starts []time.Time
	s := newTestScheduler(30*time.Millisecond, func(context.Context) error {
		if running.Add(1) > 1 {
			overlaps.Add(1)
		}
		defer running.Add(-1)
		mu.Lock()
		starts = append(starts, time.Now())
		mu.Unlock()
		time.Sleep(75 * time.Millisecond) // overruns two ticks
		return nil
	})
	runFor(t, s, 300*time.Millisecond)

	if n := overlaps.Load(); n != 0 {
		t.Fatalf("jobs overlapped %d times", n)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(starts) < 2 {
		t.Fatalf("expected at least 2 runs, got %d", len(starts))
	}
	// Job runs 75ms; ticks at +30 and +60 are dropped, so the next run is on
	// the +90 tick rather than immediately after the job finishes.
	for i := 1; i < len(starts); i++ {
		if gap := starts[i].Sub(starts[i-1]); gap < 85*time.Millisecond {
			t.Fatalf("gap %d = %v: missed tick was queued instead of dropped", i, gap)
		}
	}
}

func TestScheduler_ErrorsContinueScheduling(t *testing.T) {
	var calls, reported atomic.Int64
	errJob := errors.New("job failed")
	s := newTestScheduler(20*time.Millisecond, func(context.Context) error {
		calls.Add(1)
		return errJob
	}, WithErrorHandler(func(err error) {
		if errors.Is(err, errJob) {
			reported.Add(1)
		}
	}))
	runFor(t, s, 110*time.Millisecond)

	if n := calls.Load(); n < 3 {
		t.Fatalf("expected scheduling to continue after errors, got %d runs", n)
	}
	if reported.Load() != calls.Load() {
		t.Fatalf("reported %d of %d errors", reported.Load(), calls.Load())
	}
}

func TestScheduler_CancelDuringJob(t *testing.T) {
	var reported atomic.Int64
	started := make(chan struct{})
	s := newTestScheduler(time.Hour, func(ctx context.Context) error {
		close(started)
		<-ctx.Done()
		return ctx.Err()
	}, WithImmediateRun(), WithErrorHandler(func(error) { reported.Add(1) }))

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- s.Start(ctx) }()

	<-started
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Start returned %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Start did not return after cancel")
	}
	if n := reported.Load(); n != 0 {
		t.Fatalf("error after cancellation was reported %d times", n)
	}
}

func TestScheduler_InvalidInterval(t *testing.T) {
	for _, d := range []time.Duration{0, -time.Second} {
		s := newTestScheduler(d, func(context.Context) error { return nil })
		if err := s.Start(context.Background()); err == nil {
			t.Fatalf("interval %v: expected error", d)
		}
	}
}
