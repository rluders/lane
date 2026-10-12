package lane

import (
	"context"
	"errors"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"
)

// panicRunner panics in Start (optionally after a gate opens) and records Stop calls.
type panicRunner struct {
	name     string
	val      any
	gate     <-chan struct{} // if set, Start waits for it before acting
	startErr error           // returned instead of panicking when non-nil
	mu       *sync.Mutex
	stopped  *[]string
}

func (p *panicRunner) Name() string { return p.name }

func (p *panicRunner) Start(ctx context.Context) error {
	if p.gate != nil {
		select {
		case <-p.gate:
		case <-ctx.Done():
			return nil
		}
	}
	if p.startErr != nil {
		return p.startErr
	}
	panic(p.val)
}

func (p *panicRunner) Stop(context.Context) error {
	p.mu.Lock()
	*p.stopped = append(*p.stopped, p.name)
	p.mu.Unlock()
	return nil
}

// startedRunner blocks in Start until its context is cancelled.
type startedRunner struct {
	name    string
	started chan struct{}
	exited  chan struct{}
	mu      *sync.Mutex
	stopped *[]string
}

func newStarted(name string, mu *sync.Mutex, stopped *[]string) *startedRunner {
	return &startedRunner{name: name, started: make(chan struct{}), exited: make(chan struct{}), mu: mu, stopped: stopped}
}

func (s *startedRunner) Name() string { return s.name }

func (s *startedRunner) Start(ctx context.Context) error {
	close(s.started)
	<-ctx.Done()
	close(s.exited)
	return nil
}

func (s *startedRunner) Stop(context.Context) error {
	s.mu.Lock()
	*s.stopped = append(*s.stopped, s.name)
	s.mu.Unlock()
	return nil
}

func runWithTimeout(t *testing.T, l *Lane, ctx context.Context) error {
	t.Helper()
	done := make(chan error, 1)
	go func() { done <- l.Run(ctx) }()
	select {
	case err := <-done:
		return err
	case <-time.After(3 * time.Second):
		t.Fatal("Run deadlocked")
		return nil
	}
}

// baselineGoroutines counts goroutines after os/signal has started its
// process-lifetime watcher goroutine, which is not a leak.
func baselineGoroutines() int {
	_, stop := setupSignals()
	stop()
	time.Sleep(20 * time.Millisecond)
	return runtime.NumGoroutine()
}

func assertNoGoroutineLeak(t *testing.T, before int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		if runtime.NumGoroutine() <= before {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("goroutine leak: before=%d after=%d", before, runtime.NumGoroutine())
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func contains(list []string, s string) bool {
	for _, v := range list {
		if v == s {
			return true
		}
	}
	return false
}

func TestLane_StartupPanic_Single(t *testing.T) {
	before := baselineGoroutines()
	l, _ := newTestLane()
	var mu sync.Mutex
	var stopped []string
	l.AddRunner(&panicRunner{name: "boom", val: "kaboom", mu: &mu, stopped: &stopped})

	err := runWithTimeout(t, l, context.Background())
	if err == nil {
		t.Fatal("expected error from panicking runner")
	}
	if !strings.Contains(err.Error(), "boom") || !strings.Contains(err.Error(), "kaboom") {
		t.Errorf("error lost panic info: %v", err)
	}
	if l.Health().IsReady() {
		t.Error("lane must not be ready after startup panic")
	}
	mu.Lock()
	if !contains(stopped, "boom") {
		t.Error("panicking runner should still be stopped")
	}
	mu.Unlock()
	assertNoGoroutineLeak(t, before)
}

func TestLane_StartupPanic_ErrorValuePreserved(t *testing.T) {
	l, _ := newTestLane()
	var mu sync.Mutex
	var stopped []string
	l.AddRunner(&panicRunner{name: "boom", val: errors.New("bad init"), mu: &mu, stopped: &stopped})

	err := runWithTimeout(t, l, context.Background())
	if err == nil || !strings.Contains(err.Error(), "bad init") {
		t.Fatalf("expected panic value in error, got %v", err)
	}
}

func TestLane_StartupPanic_WhileOthersStarting(t *testing.T) {
	before := baselineGoroutines()
	l, _ := newTestLane()
	var mu sync.Mutex
	var stopped []string
	// "late" never gets its gate opened; cancellation from the panic must release it.
	late := &panicRunner{name: "late", gate: make(chan struct{}), startErr: errors.New("never"), mu: &mu, stopped: &stopped}
	slow := newStarted("slow", &mu, &stopped)
	l.AddRunner(late, slow, &panicRunner{name: "boom", val: "early", mu: &mu, stopped: &stopped})

	err := runWithTimeout(t, l, context.Background())
	if err == nil || !strings.Contains(err.Error(), "early") {
		t.Fatalf("expected panic error, got %v", err)
	}
	if l.Health().IsReady() {
		t.Error("lane must not be ready")
	}
	mu.Lock()
	for _, n := range []string{"late", "slow", "boom"} {
		if !contains(stopped, n) {
			t.Errorf("runner %s not stopped", n)
		}
	}
	mu.Unlock()
	assertNoGoroutineLeak(t, before)
}

func TestLane_StartupPanic_AfterOtherStarted(t *testing.T) {
	before := baselineGoroutines()
	l, _ := newTestLane()
	var mu sync.Mutex
	var stopped []string
	ok := newStarted("ok", &mu, &stopped)
	gate := make(chan struct{})
	go func() { <-ok.started; close(gate) }()
	l.AddRunner(ok, &panicRunner{name: "boom", val: "after", gate: gate, mu: &mu, stopped: &stopped})

	err := runWithTimeout(t, l, context.Background())
	if err == nil || !strings.Contains(err.Error(), "after") {
		t.Fatalf("expected panic error, got %v", err)
	}
	select {
	case <-ok.exited:
	default:
		t.Error("started runner's Start did not observe cancellation")
	}
	mu.Lock()
	// LIFO: boom (registered last) stops first.
	if len(stopped) != 2 || stopped[0] != "boom" || stopped[1] != "ok" {
		t.Errorf("stop order: %v", stopped)
	}
	mu.Unlock()
	if l.Health().IsReady() {
		t.Error("lane must not be ready")
	}
	assertNoGoroutineLeak(t, before)
}

func TestLane_StartupPanic_Multiple(t *testing.T) {
	before := baselineGoroutines()
	l, _ := newTestLane()
	var mu sync.Mutex
	var stopped []string
	l.AddRunner(
		&panicRunner{name: "p1", val: "one", mu: &mu, stopped: &stopped},
		&panicRunner{name: "p2", val: "two", mu: &mu, stopped: &stopped},
		&panicRunner{name: "p3", val: "three", mu: &mu, stopped: &stopped},
	)

	err := runWithTimeout(t, l, context.Background())
	if err == nil || !strings.Contains(err.Error(), "panicked") {
		t.Fatalf("expected a panic error, got %v", err)
	}
	mu.Lock()
	if len(stopped) != 3 {
		t.Errorf("all runners should be stopped once, got %v", stopped)
	}
	mu.Unlock()
	if l.Health().IsReady() {
		t.Error("lane must not be ready")
	}
	assertNoGoroutineLeak(t, before)
}

func TestLane_StartupPanic_WithStartError(t *testing.T) {
	before := baselineGoroutines()
	l, _ := newTestLane()
	var mu sync.Mutex
	var stopped []string
	sentinel := errors.New("start failed")
	l.AddRunner(
		&panicRunner{name: "boom", val: "kaboom", mu: &mu, stopped: &stopped},
		&panicRunner{name: "err", startErr: sentinel, mu: &mu, stopped: &stopped},
	)

	err := runWithTimeout(t, l, context.Background())
	if err == nil {
		t.Fatal("expected error")
	}
	// First failure wins; it must be exactly one of the two.
	isPanic := strings.Contains(err.Error(), "kaboom")
	isStart := errors.Is(err, sentinel)
	if isPanic == isStart {
		t.Errorf("expected exactly one of panic or start error, got %v", err)
	}
	mu.Lock()
	if len(stopped) != 2 {
		t.Errorf("both runners should be stopped, got %v", stopped)
	}
	mu.Unlock()
	if l.Health().IsReady() {
		t.Error("lane must not be ready")
	}
	assertNoGoroutineLeak(t, before)
}

func TestLane_StartupPanic_WithContextCancel(t *testing.T) {
	before := baselineGoroutines()
	l, _ := newTestLane()
	var mu sync.Mutex
	var stopped []string
	gate := make(chan struct{})
	ok := newStarted("ok", &mu, &stopped)
	l.AddRunner(ok, &panicRunner{name: "boom", val: "kaboom", gate: gate, mu: &mu, stopped: &stopped})

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		<-ok.started
		// Release the panic and cancel the parent at (nearly) the same time.
		close(gate)
		cancel()
	}()

	err := runWithTimeout(t, l, ctx)
	// The panic may or may not win the race with cancellation; if an error is
	// returned it must be the panic.
	if err != nil && !strings.Contains(err.Error(), "kaboom") {
		t.Errorf("unexpected error: %v", err)
	}
	mu.Lock()
	if len(stopped) != 2 {
		t.Errorf("both runners should be stopped, got %v", stopped)
	}
	mu.Unlock()
	if l.Health().IsReady() {
		t.Error("lane must not be ready")
	}
	assertNoGoroutineLeak(t, before)
}
