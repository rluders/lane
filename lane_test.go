package lane

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strings"
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

	if l.Health().Ready() {
		t.Fatal("should not be ready before Run")
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- l.Run(ctx) }()

	time.Sleep(50 * time.Millisecond)
	if !l.Health().Ready() {
		t.Fatal("should be ready after runners started")
	}

	cancel()

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("timeout")
	}

	if l.Health().Ready() {
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
	// alpha is reached after the deadline: it still gets Stop, but Run does
	// not wait for it.
	waitStopped(t, &mu, &stopOrder, 2)
	assertStopOrder(t, &mu, stopOrder, "beta", "alpha")
}

func waitStopped(t *testing.T, mu *sync.Mutex, stopOrder *[]string, n int) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for {
		mu.Lock()
		got := len(*stopOrder)
		mu.Unlock()
		if got >= n {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("only %d of %d runners received Stop", got, n)
		}
		time.Sleep(5 * time.Millisecond)
	}
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

// readyRunner is a fakeRunner that also implements Readier.
type readyRunner struct {
	*fakeRunner
	ready    chan struct{} // Ready returns nil once closed
	readyErr error
}

func newReadyFake(name string, stopOrder *[]string, mu *sync.Mutex) *readyRunner {
	return &readyRunner{fakeRunner: newFake(name, stopOrder, mu), ready: make(chan struct{})}
}

func (r *readyRunner) Ready(ctx context.Context) error {
	if r.readyErr != nil {
		return r.readyErr
	}
	select {
	case <-r.ready:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// startLane runs l in the background and returns a cancel func and Run's result channel.
func startLane(l *Lane) (context.CancelFunc, <-chan error) {
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- l.Run(ctx) }()
	return cancel, done
}

func waitRun(t *testing.T, done <-chan error) error {
	t.Helper()
	select {
	case err := <-done:
		return err
	case <-time.After(3 * time.Second):
		t.Fatal("l.Run did not return in time")
		return nil
	}
}

func waitReady(t *testing.T, l *Lane) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for !l.Health().Ready() {
		if time.Now().After(deadline) {
			t.Fatal("lane did not become ready")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestLane_ReadyWaitsForAllReadiers(t *testing.T) {
	l, _ := newTestLane()
	var stopOrder []string
	var mu sync.Mutex

	plain := newFake("plain", &stopOrder, &mu)
	a := newReadyFake("a", &stopOrder, &mu)
	b := newReadyFake("b", &stopOrder, &mu)
	l.AddRunner(plain, a, b)

	cancel, done := startLane(l)
	defer cancel()

	time.Sleep(50 * time.Millisecond)
	if l.Health().Ready() {
		t.Fatal("ready before any Readier reported ready")
	}

	close(a.ready)
	time.Sleep(50 * time.Millisecond)
	if l.Health().Ready() {
		t.Fatal("ready before all Readiers reported ready")
	}

	close(b.ready)
	waitReady(t, l)

	cancel()
	if err := waitRun(t, done); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if l.Health().Ready() {
		t.Fatal("still ready after shutdown")
	}
}

func TestLane_ReadyErrorFailsRun(t *testing.T) {
	l, _ := newTestLane()
	var stopOrder []string
	var mu sync.Mutex

	errNotReady := errors.New("broker unreachable")
	r := newReadyFake("broker", &stopOrder, &mu)
	r.readyErr = errNotReady
	other := newFake("other", &stopOrder, &mu)
	l.AddRunner(other, r)

	_, done := startLane(l)
	err := waitRun(t, done)
	if !errors.Is(err, errNotReady) {
		t.Fatalf("expected readiness error, got %v", err)
	}
	if l.Health().Ready() {
		t.Fatal("ready after readiness failure")
	}
	assertStopOrder(t, &mu, stopOrder, "broker", "other")
}

func TestLane_StartFailureBeforeReadyPreventsReady(t *testing.T) {
	l, _ := newTestLane()
	var stopOrder []string
	var mu sync.Mutex

	errStart := errors.New("db start failed")
	failing := newFake("db", &stopOrder, &mu)
	failing.startErr = errStart
	pending := newReadyFake("http", &stopOrder, &mu) // never becomes ready
	l.AddRunner(failing, pending)

	_, done := startLane(l)
	err := waitRun(t, done)
	if !errors.Is(err, errStart) {
		t.Fatalf("expected start error, got %v", err)
	}
	if errors.Is(err, context.Canceled) {
		t.Fatalf("readiness cancellation leaked into result: %v", err)
	}
	if l.Health().Ready() {
		t.Fatal("ready after startup failure")
	}
}

func TestLane_CancelDuringStartupPreventsReady(t *testing.T) {
	l, _ := newTestLane()
	var stopOrder []string
	var mu sync.Mutex

	pending := newReadyFake("http", &stopOrder, &mu) // never becomes ready
	l.AddRunner(pending)

	cancel, done := startLane(l)
	time.Sleep(50 * time.Millisecond)
	cancel()

	if err := waitRun(t, done); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if l.Health().Ready() {
		t.Fatal("ready after cancellation during startup")
	}
}

// panicRunner is a fakeRunner that panics in the configured methods.
type panicRunner struct {
	*fakeRunner
	panicStart, panicStop bool
}

func (p *panicRunner) Start(ctx context.Context) error {
	if p.panicStart {
		panic("boom in start")
	}
	return p.fakeRunner.Start(ctx)
}

func (p *panicRunner) Stop(ctx context.Context) error {
	err := p.fakeRunner.Stop(ctx)
	if p.panicStop {
		panic("boom in stop")
	}
	return err
}

type panicReadier struct{ *fakeRunner }

func (p *panicReadier) Ready(ctx context.Context) error { panic("boom in ready") }

func assertPanicError(t *testing.T, err error, runner, op string) {
	t.Helper()
	var pe *PanicError
	if !errors.As(err, &pe) {
		t.Fatalf("expected *PanicError, got %v", err)
	}
	if pe.Runner != runner || pe.Op != op {
		t.Fatalf("PanicError{Runner: %q, Op: %q}, want {%q, %q}", pe.Runner, pe.Op, runner, op)
	}
	if len(pe.Stack) == 0 {
		t.Fatal("PanicError has no stack trace")
	}
}

func TestLane_PanicInStartTriggersShutdown(t *testing.T) {
	l, _ := newTestLane()
	var stopOrder []string
	var mu sync.Mutex

	ok := newFake("ok", &stopOrder, &mu)
	p := &panicRunner{fakeRunner: newFake("bad", &stopOrder, &mu), panicStart: true}
	l.AddRunner(ok, p)

	_, done := startLane(l)
	err := waitRun(t, done)
	assertPanicError(t, err, "bad", "start")
	if l.Health().Ready() {
		t.Fatal("ready after panic in start")
	}
	assertStopOrder(t, &mu, stopOrder, "bad", "ok")
}

func TestLane_PanicInStopContinuesShutdown(t *testing.T) {
	l, _ := newTestLane()
	var stopOrder []string
	var mu sync.Mutex

	a := newFake("alpha", &stopOrder, &mu)
	p := &panicRunner{fakeRunner: newFake("bad", &stopOrder, &mu), panicStop: true}
	c := newFake("gamma", &stopOrder, &mu)
	l.AddRunner(a, p, c)

	err := runUntilCancelled(t, l, 3*time.Second)
	assertPanicError(t, err, "bad", "stop")
	assertStopOrder(t, &mu, stopOrder, "gamma", "bad", "alpha")
}

func TestLane_PanicInReadyTriggersShutdown(t *testing.T) {
	l, _ := newTestLane()
	var stopOrder []string
	var mu sync.Mutex

	p := &panicReadier{newFake("bad", &stopOrder, &mu)}
	l.AddRunner(p)

	_, done := startLane(l)
	err := waitRun(t, done)
	assertPanicError(t, err, "bad", "ready")
	if l.Health().Ready() {
		t.Fatal("ready after panic in ready")
	}
}

func TestLane_ConcurrentPanicsInStart(t *testing.T) {
	l, _ := newTestLane()
	var stopOrder []string
	var mu sync.Mutex

	for _, name := range []string{"a", "b", "c"} {
		l.AddRunner(&panicRunner{fakeRunner: newFake(name, &stopOrder, &mu), panicStart: true})
	}

	_, done := startLane(l)
	err := waitRun(t, done)
	var pe *PanicError
	if !errors.As(err, &pe) || pe.Op != "start" {
		t.Fatalf("expected start *PanicError, got %v", err)
	}
	assertStopOrder(t, &mu, stopOrder, "c", "b", "a")
}

// latePanicRunner panics in Start after the lane has become ready.
type latePanicRunner struct {
	*fakeRunner
	trigger chan struct{}
}

func (p *latePanicRunner) Start(ctx context.Context) error {
	<-p.trigger
	panic("boom at runtime")
}

func TestLane_PanicAtRuntimeTriggersShutdown(t *testing.T) {
	l, _ := newTestLane()
	var stopOrder []string
	var mu sync.Mutex

	ok := newFake("ok", &stopOrder, &mu)
	p := &latePanicRunner{fakeRunner: newFake("bad", &stopOrder, &mu), trigger: make(chan struct{})}
	l.AddRunner(ok, p)

	_, done := startLane(l)
	waitReady(t, l)
	close(p.trigger)

	err := waitRun(t, done)
	assertPanicError(t, err, "bad", "start")
	if l.Health().Ready() {
		t.Fatal("still ready after runtime panic")
	}
	assertStopOrder(t, &mu, stopOrder, "bad", "ok")
}

// stuckRunner ignores its contexts: Start ignores cancellation (if stuckStart)
// and Stop ignores its deadline (if stuckStop). Both unblock on release.
type stuckRunner struct {
	*fakeRunner
	stuckStart, stuckStop bool
	release               chan struct{}
}

func newStuck(t *testing.T, name string, stopOrder *[]string, mu *sync.Mutex) *stuckRunner {
	s := &stuckRunner{fakeRunner: newFake(name, stopOrder, mu), release: make(chan struct{})}
	t.Cleanup(func() { close(s.release) })
	return s
}

func (s *stuckRunner) Start(ctx context.Context) error {
	if s.stuckStart {
		<-s.release
		return nil
	}
	return s.fakeRunner.Start(ctx)
}

func (s *stuckRunner) Stop(ctx context.Context) error {
	if s.stuckStop {
		s.mu.Lock()
		*s.stopOrder = append(*s.stopOrder, s.name)
		s.mu.Unlock()
		<-s.release
		return nil
	}
	return s.fakeRunner.Stop(ctx)
}

func TestLane_StopIgnoringDeadlineDoesNotBlock(t *testing.T) {
	log := slog.New(slog.NewTextHandler(os.Stderr, nil))
	l := New(log, WithShutdownTimeout(100*time.Millisecond))

	var stopOrder []string
	var mu sync.Mutex

	a := newFake("alpha", &stopOrder, &mu)
	b := newStuck(t, "beta", &stopOrder, &mu)
	b.stuckStop = true
	l.AddRunner(a, b)

	start := time.Now()
	err := runUntilCancelled(t, l, 2*time.Second)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected deadline exceeded, got %v", err)
	}
	if d := time.Since(start); d > time.Second {
		t.Fatalf("shutdown took %v, want bounded by timeout", d)
	}
	waitStopped(t, &mu, &stopOrder, 2)
	assertStopOrder(t, &mu, stopOrder, "beta", "alpha")
}

func TestLane_StartIgnoringCancellationDoesNotBlock(t *testing.T) {
	log := slog.New(slog.NewTextHandler(os.Stderr, nil))
	l := New(log, WithShutdownTimeout(100*time.Millisecond))

	var stopOrder []string
	var mu sync.Mutex

	a := newFake("alpha", &stopOrder, &mu)
	b := newStuck(t, "beta", &stopOrder, &mu)
	b.stuckStart = true
	l.AddRunner(a, b)

	start := time.Now()
	err := runUntilCancelled(t, l, 2*time.Second)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected deadline exceeded, got %v", err)
	}
	if !strings.Contains(err.Error(), "beta") || strings.Contains(err.Error(), "alpha") {
		t.Fatalf("error should name only the stuck runner: %v", err)
	}
	if d := time.Since(start); d > time.Second {
		t.Fatalf("shutdown took %v, want bounded by timeout", d)
	}
}

func TestLane_TimeoutKeepsFirstRunnerError(t *testing.T) {
	log := slog.New(slog.NewTextHandler(os.Stderr, nil))
	l := New(log, WithShutdownTimeout(100*time.Millisecond))

	var stopOrder []string
	var mu sync.Mutex

	errStart := errors.New("start failed")
	a := newFake("alpha", &stopOrder, &mu)
	a.startErr = errStart
	b := newStuck(t, "beta", &stopOrder, &mu)
	b.stuckStart = true
	l.AddRunner(a, b)

	_, done := startLane(l)
	err := waitRun(t, done)
	if !errors.Is(err, errStart) || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected start error and deadline exceeded, got %v", err)
	}
}

// ctxErrRunner blocks until ctx is done, then returns err(ctx) — the common
// Go idiom of reporting cancellation as an error.
type ctxErrRunner struct {
	*fakeRunner
	err func(ctx context.Context) error
}

func (c *ctxErrRunner) Start(ctx context.Context) error {
	<-ctx.Done()
	return c.err(ctx)
}

func newCtxErr(name string, stopOrder *[]string, mu *sync.Mutex, err func(context.Context) error) *ctxErrRunner {
	return &ctxErrRunner{fakeRunner: newFake(name, stopOrder, mu), err: err}
}

func TestLane_ContextErrorOnShutdownIsClean(t *testing.T) {
	cases := map[string]func(context.Context) error{
		"canceled":         func(ctx context.Context) error { return ctx.Err() },
		"wrapped canceled": func(ctx context.Context) error { return fmt.Errorf("consumer: %w", ctx.Err()) },
		"deadline":         func(context.Context) error { return context.DeadlineExceeded },
	}
	for name, errFn := range cases {
		t.Run(name, func(t *testing.T) {
			l, _ := newTestLane()
			var stopOrder []string
			var mu sync.Mutex
			l.AddRunner(newCtxErr("idiomatic", &stopOrder, &mu, errFn))

			if err := runUntilCancelled(t, l, 3*time.Second); err != nil {
				t.Fatalf("clean shutdown returned %v", err)
			}
		})
	}
}

func TestLane_ContextErrorBeforeShutdownIsFailure(t *testing.T) {
	l, _ := newTestLane()
	var stopOrder []string
	var mu sync.Mutex

	r := newFake("spurious", &stopOrder, &mu)
	r.startErr = context.Canceled // returned while Lane is still running
	l.AddRunner(r)

	_, done := startLane(l)
	if err := waitRun(t, done); !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled failure, got %v", err)
	}
}

func TestLane_PrimaryFailureNotMaskedByCancellation(t *testing.T) {
	l, _ := newTestLane()
	var stopOrder []string
	var mu sync.Mutex

	errDB := errors.New("db connection lost")
	failing := newFake("db", &stopOrder, &mu)
	failing.startErr = errDB
	l.AddRunner(
		failing,
		newCtxErr("a", &stopOrder, &mu, func(ctx context.Context) error { return ctx.Err() }),
		newCtxErr("b", &stopOrder, &mu, func(ctx context.Context) error { return ctx.Err() }),
	)

	_, done := startLane(l)
	err := waitRun(t, done)
	if !errors.Is(err, errDB) {
		t.Fatalf("expected primary failure, got %v", err)
	}
	if errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation of other runners leaked into result: %v", err)
	}
}

func TestLane_NonContextErrorDuringShutdownIsFailure(t *testing.T) {
	l, _ := newTestLane()
	var stopOrder []string
	var mu sync.Mutex

	errFlush := errors.New("flush failed")
	l.AddRunner(newCtxErr("flusher", &stopOrder, &mu, func(context.Context) error { return errFlush }))

	if err := runUntilCancelled(t, l, 3*time.Second); !errors.Is(err, errFlush) {
		t.Fatalf("expected flush error, got %v", err)
	}
}

func TestLane_RunnerReturningNilEarlyKeepsLaneRunning(t *testing.T) {
	l, _ := newTestLane()
	var stopOrder []string
	var mu sync.Mutex

	oneShot := newFake("migrate", &stopOrder, &mu)
	server := newFake("server", &stopOrder, &mu)
	l.AddRunner(&nilStarter{oneShot}, server)

	cancel, done := startLane(l)
	defer cancel()
	waitReady(t, l)

	time.Sleep(50 * time.Millisecond)
	select {
	case err := <-done:
		t.Fatalf("Run returned early (%v) after a runner finished cleanly", err)
	default:
	}
	if !l.Health().Ready() {
		t.Fatal("not ready after a runner finished cleanly")
	}

	cancel()
	if err := waitRun(t, done); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertStopOrder(t, &mu, stopOrder, "server", "migrate")
}

// nilStarter returns nil from Start immediately.
type nilStarter struct{ *fakeRunner }

func (n *nilStarter) Start(context.Context) error { return nil }

// stateRecorder records the Lane's health while being stopped.
type stateRecorder struct {
	*fakeRunner
	l         *Lane
	stopState State
	stopLive  bool
	stopReady bool
}

func (s *stateRecorder) Stop(ctx context.Context) error {
	h := s.l.Health()
	s.stopState, s.stopLive, s.stopReady = h.State(), h.Live(), h.Ready()
	return s.fakeRunner.Stop(ctx)
}

func TestLane_HealthStatesThroughLifecycle(t *testing.T) {
	l, _ := newTestLane()
	var stopOrder []string
	var mu sync.Mutex

	rd := newReadyFake("http", &stopOrder, &mu)
	rec := &stateRecorder{fakeRunner: newFake("rec", &stopOrder, &mu), l: l}
	l.AddRunner(rec, rd)

	if s := l.Health().State(); s != StateStarting || !l.Health().Live() {
		t.Fatalf("before Run: %v live=%v", s, l.Health().Live())
	}

	cancel, done := startLane(l)
	time.Sleep(30 * time.Millisecond)
	if s := l.Health().State(); s != StateStarting || l.Health().Ready() {
		t.Fatalf("while starting: %v ready=%v", s, l.Health().Ready())
	}

	close(rd.ready)
	waitReady(t, l)
	if l.Health().State() != StateRunning || !l.Health().Live() {
		t.Fatalf("running: %v live=%v", l.Health().State(), l.Health().Live())
	}

	cancel()
	if err := waitRun(t, done); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if rec.stopState != StateStopping || !rec.stopLive || rec.stopReady {
		t.Fatalf("during shutdown: %v live=%v ready=%v", rec.stopState, rec.stopLive, rec.stopReady)
	}
	if s := l.Health().State(); s != StateStopped || l.Health().Live() || l.Health().Ready() {
		t.Fatalf("after Run: %v live=%v ready=%v", s, l.Health().Live(), l.Health().Ready())
	}
}

func TestLane_HealthStartupFailureSkipsRunning(t *testing.T) {
	l, _ := newTestLane()
	var stopOrder []string
	var mu sync.Mutex

	failing := newFake("db", &stopOrder, &mu)
	failing.startErr = errors.New("boom")
	rec := &stateRecorder{fakeRunner: newFake("rec", &stopOrder, &mu), l: l}
	l.AddRunner(rec, failing)

	_, done := startLane(l)
	if err := waitRun(t, done); err == nil {
		t.Fatal("expected error")
	}
	if rec.stopState != StateStopping || rec.stopReady {
		t.Fatalf("during shutdown: %v ready=%v", rec.stopState, rec.stopReady)
	}
	if l.Health().State() != StateStopped {
		t.Fatalf("after Run: %v", l.Health().State())
	}
}

// reusableRunner blocks until cancelled and can be stopped any number of times.
type reusableRunner struct{}

func (reusableRunner) Name() string                    { return "reusable" }
func (reusableRunner) Start(ctx context.Context) error { <-ctx.Done(); return nil }
func (reusableRunner) Stop(context.Context) error      { return nil }

func TestLane_RunAgainResetsHealth(t *testing.T) {
	l, _ := newTestLane()
	l.AddRunner(reusableRunner{})

	for i := range 2 {
		cancel, done := startLane(l)
		waitReady(t, l)
		cancel()
		if err := waitRun(t, done); err != nil {
			t.Fatalf("run %d: %v", i, err)
		}
		if l.Health().State() != StateStopped {
			t.Fatalf("run %d: ended in %v", i, l.Health().State())
		}
	}
}
