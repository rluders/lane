package lane

import (
	"context"
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
