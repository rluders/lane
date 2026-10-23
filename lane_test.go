package lane

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"sync"
	"testing"
	"time"
)

// fakeRunner is a controllable Runner for testing.
type fakeRunner struct {
	name      string
	startErr  error
	stopErr   error
	stopOrder *[]string
	mu        *sync.Mutex
	block     chan struct{} // Start blocks until this is closed
	stopWait  bool          // Stop blocks until its ctx is done
}

func newFake(name string, stopOrder *[]string, mu *sync.Mutex) *fakeRunner {
	return &fakeRunner{
		name:      name,
		stopOrder: stopOrder,
		mu:        mu,
		block:     make(chan struct{}),
	}
}

func (f *fakeRunner) Name() string { return f.name }

func (f *fakeRunner) Start(ctx context.Context) error {
	if f.startErr != nil {
		return f.startErr
	}
	select {
	case <-ctx.Done():
	case <-f.block:
	}
	return nil
}

func (f *fakeRunner) Stop(ctx context.Context) error {
	f.mu.Lock()
	*f.stopOrder = append(*f.stopOrder, f.name)
	f.mu.Unlock()
	close(f.block)
	if f.stopWait {
		<-ctx.Done()
		return ctx.Err()
	}
	return f.stopErr
}

func newTestLane() (*Lane, *slog.Logger) {
	log := slog.New(slog.NewTextHandler(os.Stderr, nil))
	return New(log, WithShutdownTimeout(2*time.Second)), log
}

func TestLane_ShutdownLIFO(t *testing.T) {
	l, _ := newTestLane()

	var stopOrder []string
	var mu sync.Mutex

	a := newFake("alpha", &stopOrder, &mu)
	b := newFake("beta", &stopOrder, &mu)
	c := newFake("gamma", &stopOrder, &mu)

	l.AddRunner(a, b, c)

	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan error, 1)
	go func() { done <- l.Run(ctx) }()

	// Wait for health to be ready (all runners started)
	time.Sleep(50 * time.Millisecond)

	cancel() // trigger shutdown via context cancellation

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("l.Run did not return in time")
	}

	mu.Lock()
	defer mu.Unlock()

	want := []string{"gamma", "beta", "alpha"}
	if len(stopOrder) != len(want) {
		t.Fatalf("stop order len: got %d, want %d", len(stopOrder), len(want))
	}
	for i, name := range want {
		if stopOrder[i] != name {
			t.Errorf("stop order[%d]: got %q, want %q", i, stopOrder[i], name)
		}
	}
}

func TestLane_HealthTransitions(t *testing.T) {
	l, _ := newTestLane()

	var stopOrder []string
	var mu sync.Mutex
	r := newFake("solo", &stopOrder, &mu)
	l.AddRunner(r)

	if l.Health().IsReady() {
		t.Fatal("should not be ready before Run")
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- l.Run(ctx) }()

	time.Sleep(50 * time.Millisecond)
	if !l.Health().IsReady() {
		t.Fatal("should be ready after runners started")
	}

	cancel()

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("timeout")
	}

	if l.Health().IsReady() {
		t.Fatal("should not be ready after shutdown")
	}
}

func TestLane_RunnerError_PropagatesShutdown(t *testing.T) {
	l, _ := newTestLane()

	var stopOrder []string
	var mu sync.Mutex

	errRunner := &fakeRunner{
		name:      "failing",
		startErr:  context.DeadlineExceeded, // immediate error
		stopOrder: &stopOrder,
		mu:        &mu,
		block:     make(chan struct{}),
	}
	l.AddRunner(errRunner)

	err := l.Run(context.Background())
	if err == nil {
		t.Fatal("expected error from failing runner")
	}
}

// runUntilCancelled runs l, cancels it once runners have started, and returns Run's error.
func runUntilCancelled(t *testing.T, l *Lane, timeout time.Duration) error {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- l.Run(ctx) }()

	time.Sleep(50 * time.Millisecond)
	cancel()

	select {
	case err := <-done:
		return err
	case <-time.After(timeout):
		t.Fatal("l.Run did not return in time")
		return nil
	}
}

func assertStopOrder(t *testing.T, mu *sync.Mutex, got []string, want ...string) {
	t.Helper()
	mu.Lock()
	defer mu.Unlock()
	if len(got) != len(want) {
		t.Fatalf("stop order: got %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("stop order: got %v, want %v", got, want)
		}
	}
}

func TestLane_StopErrorDoesNotAbortShutdown(t *testing.T) {
	l, _ := newTestLane()

	var stopOrder []string
	var mu sync.Mutex
	errGamma := errors.New("gamma stop failed")

	a := newFake("alpha", &stopOrder, &mu)
	b := newFake("beta", &stopOrder, &mu)
	c := newFake("gamma", &stopOrder, &mu)
	c.stopErr = errGamma
	l.AddRunner(a, b, c)

	err := runUntilCancelled(t, l, 3*time.Second)
	if !errors.Is(err, errGamma) {
		t.Fatalf("expected gamma stop error, got %v", err)
	}
	assertStopOrder(t, &mu, stopOrder, "gamma", "beta", "alpha")
}

func TestLane_MultipleStopErrorsAreJoined(t *testing.T) {
	l, _ := newTestLane()

	var stopOrder []string
	var mu sync.Mutex
	errAlpha := errors.New("alpha stop failed")
	errGamma := errors.New("gamma stop failed")

	a := newFake("alpha", &stopOrder, &mu)
	b := newFake("beta", &stopOrder, &mu)
	c := newFake("gamma", &stopOrder, &mu)
	a.stopErr = errAlpha
	c.stopErr = errGamma
	l.AddRunner(a, b, c)

	err := runUntilCancelled(t, l, 3*time.Second)
	if !errors.Is(err, errAlpha) || !errors.Is(err, errGamma) {
		t.Fatalf("expected both stop errors, got %v", err)
	}
	assertStopOrder(t, &mu, stopOrder, "gamma", "beta", "alpha")
}

func TestLane_StopContinuesAfterShutdownTimeout(t *testing.T) {
	log := slog.New(slog.NewTextHandler(os.Stderr, nil))
	l := New(log, WithShutdownTimeout(100*time.Millisecond))

	var stopOrder []string
	var mu sync.Mutex

	a := newFake("alpha", &stopOrder, &mu)
	b := newFake("beta", &stopOrder, &mu)
	b.stopWait = true // consumes the whole shutdown timeout
	l.AddRunner(a, b)

	start := time.Now()
	err := runUntilCancelled(t, l, 3*time.Second)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected deadline exceeded, got %v", err)
	}
	if d := time.Since(start); d > time.Second {
		t.Fatalf("shutdown took %v, want bounded by timeout", d)
	}
	assertStopOrder(t, &mu, stopOrder, "beta", "alpha")
}

func TestLane_StartAndStopErrorsAreJoined(t *testing.T) {
	l, _ := newTestLane()

	var stopOrder []string
	var mu sync.Mutex
	errStart := errors.New("start failed")
	errStop := errors.New("stop failed")

	r := newFake("solo", &stopOrder, &mu)
	r.startErr = errStart
	r.stopErr = errStop
	l.AddRunner(r)

	err := l.Run(context.Background())
	if !errors.Is(err, errStart) || !errors.Is(err, errStop) {
		t.Fatalf("expected start and stop errors, got %v", err)
	}
}
