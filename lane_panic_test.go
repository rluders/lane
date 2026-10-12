package lane

import (
	"context"
	"errors"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
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

// readiness observes the lane's readiness while Run is in progress. Lane sets
// ready right after spawning the runners and withdraws it before any Stop call
// once a runner fails or panics, so the contract verified here is:
//   - readiness is already withdrawn when the first Stop is invoked,
//   - readiness never flaps (once withdrawn it is not set again),
//   - the lane is not ready after Run returns.
//
// A brief ready window between spawning runners and the panic surfacing is
// allowed by the current semantics and is deliberately not asserted against.
type readiness struct {
	l         *Lane
	stopPoll  chan struct{}
	pollDone  chan struct{}
	stopSawOK atomic.Bool // a Stop observed the lane still ready
	flapped   atomic.Bool // ready was observed again after being withdrawn
	sawReady  atomic.Bool
}

func watchReadiness(l *Lane) *readiness {
	r := &readiness{l: l, stopPoll: make(chan struct{}), pollDone: make(chan struct{})}
	go func() {
		defer close(r.pollDone)
		withdrawn := false
		for {
			select {
			case <-r.stopPoll:
				return
			default:
			}
			if l.Health().IsReady() {
				r.sawReady.Store(true)
				if withdrawn {
					r.flapped.Store(true)
				}
			} else if r.sawReady.Load() {
				withdrawn = true
			}
			time.Sleep(time.Millisecond)
		}
	}()
	return r
}

type readyCheckRunner struct {
	Runner
	r *readiness
}

func (c readyCheckRunner) Stop(ctx context.Context) error {
	if c.r.l.Health().IsReady() {
		c.r.stopSawOK.Store(true)
	}
	return c.Runner.Stop(ctx)
}

func (r *readiness) add(runners ...Runner) {
	for _, run := range runners {
		r.l.AddRunner(readyCheckRunner{Runner: run, r: r})
	}
}

// verify must be called after Run has returned.
func (r *readiness) verify(t *testing.T) {
	t.Helper()
	close(r.stopPoll)
	<-r.pollDone
	if r.stopSawOK.Load() {
		t.Error("lane was still ready when a runner was stopped after a startup panic")
	}
	if r.flapped.Load() {
		t.Error("lane became ready again after readiness was withdrawn")
	}
	if r.l.Health().IsReady() {
		t.Error("lane must not be ready after Run returns")
	}
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
	rd := watchReadiness(l)
	var mu sync.Mutex
	var stopped []string
	rd.add(&panicRunner{name: "boom", val: "kaboom", mu: &mu, stopped: &stopped})

	err := runWithTimeout(t, l, context.Background())
	if err == nil {
		t.Fatal("expected error from panicking runner")
	}
	if !strings.Contains(err.Error(), "boom") || !strings.Contains(err.Error(), "kaboom") {
		t.Errorf("error lost panic info: %v", err)
	}
	rd.verify(t)
	mu.Lock()
	if !contains(stopped, "boom") {
		t.Error("panicking runner should still be stopped")
	}
	mu.Unlock()
	assertNoGoroutineLeak(t, before)
}

func TestLane_StartupPanic_ErrorValuePreserved(t *testing.T) {
	l, _ := newTestLane()
	rd := watchReadiness(l)
	var mu sync.Mutex
	var stopped []string
	rd.add(&panicRunner{name: "boom", val: errors.New("bad init"), mu: &mu, stopped: &stopped})

	err := runWithTimeout(t, l, context.Background())
	if err == nil || !strings.Contains(err.Error(), "bad init") {
		t.Fatalf("expected panic value in error, got %v", err)
	}
	rd.verify(t)
}

func TestLane_StartupPanic_WhileOthersStarting(t *testing.T) {
	before := baselineGoroutines()
	l, _ := newTestLane()
	rd := watchReadiness(l)
	var mu sync.Mutex
	var stopped []string
	// "late" never gets its gate opened; cancellation from the panic must release it.
	late := &panicRunner{name: "late", gate: make(chan struct{}), startErr: errors.New("never"), mu: &mu, stopped: &stopped}
	slow := newStarted("slow", &mu, &stopped)
	rd.add(late, slow, &panicRunner{name: "boom", val: "early", mu: &mu, stopped: &stopped})

	err := runWithTimeout(t, l, context.Background())
	if err == nil || !strings.Contains(err.Error(), "early") {
		t.Fatalf("expected panic error, got %v", err)
	}
	rd.verify(t)
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
	rd := watchReadiness(l)
	var mu sync.Mutex
	var stopped []string
	ok := newStarted("ok", &mu, &stopped)
	gate := make(chan struct{})
	go func() { <-ok.started; close(gate) }()
	rd.add(ok, &panicRunner{name: "boom", val: "after", gate: gate, mu: &mu, stopped: &stopped})

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
	rd.verify(t)
	assertNoGoroutineLeak(t, before)
}

func TestLane_StartupPanic_Multiple(t *testing.T) {
	before := baselineGoroutines()
	l, _ := newTestLane()
	rd := watchReadiness(l)
	var mu sync.Mutex
	var stopped []string
	rd.add(
		&panicRunner{name: "p1", val: "one", mu: &mu, stopped: &stopped},
		&panicRunner{name: "p2", val: "two", mu: &mu, stopped: &stopped},
		&panicRunner{name: "p3", val: "three", mu: &mu, stopped: &stopped},
	)

	err := runWithTimeout(t, l, context.Background())
	if err == nil {
		t.Fatal("expected a panic error")
	}
	// errgroup keeps the first error: exactly one runner's panic is reported.
	reported := 0
	for _, name := range []string{"p1", "p2", "p3"} {
		if strings.Contains(err.Error(), `runner "`+name+`" panicked`) {
			reported++
		}
	}
	if reported != 1 {
		t.Errorf("expected exactly one runner panic in error, got %v", err)
	}
	mu.Lock()
	if len(stopped) != 3 {
		t.Errorf("all runners should be stopped once, got %v", stopped)
	}
	mu.Unlock()
	rd.verify(t)
	assertNoGoroutineLeak(t, before)
}

func TestLane_StartupPanic_WithStartError(t *testing.T) {
	before := baselineGoroutines()
	l, _ := newTestLane()
	rd := watchReadiness(l)
	var mu sync.Mutex
	var stopped []string
	sentinel := errors.New("start failed")
	rd.add(
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
	rd.verify(t)
	assertNoGoroutineLeak(t, before)
}

func TestLane_StartupPanic_WithContextCancel(t *testing.T) {
	before := baselineGoroutines()
	l, _ := newTestLane()
	rd := watchReadiness(l)
	var mu sync.Mutex
	var stopped []string
	gate := make(chan struct{})
	ok := newStarted("ok", &mu, &stopped)
	rd.add(ok, &panicRunner{name: "boom", val: "kaboom", gate: gate, mu: &mu, stopped: &stopped})

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		<-ok.started
		// Release the panic and cancel the parent at (nearly) the same time.
		close(gate)
		cancel()
	}()

	err := runWithTimeout(t, l, ctx)
	// The panic races with cancellation: the gated runner returns nil if it sees
	// the cancelled context first, otherwise it panics. Either outcome is valid,
	// but a returned error must be the panic and nothing else.
	if err != nil && !strings.Contains(err.Error(), `runner "boom" panicked: kaboom`) {
		t.Errorf("unexpected error: %v", err)
	}
	select {
	case <-ok.exited:
	default:
		t.Error("started runner's Start did not observe cancellation")
	}
	mu.Lock()
	if len(stopped) != 2 {
		t.Errorf("both runners should be stopped, got %v", stopped)
	}
	mu.Unlock()
	rd.verify(t)
	assertNoGoroutineLeak(t, before)
}
