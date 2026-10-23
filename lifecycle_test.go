package lane

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

// probeRunner is a configurable Runner for lifecycle tests. All hooks are
// optional; by default Start blocks until ctx is done and Stop returns nil.
type probeRunner struct {
	name    string
	start   func(ctx context.Context) error
	stop    func(ctx context.Context) error
	started atomic.Int32
	stopped atomic.Int32
	stopCtx atomic.Value // context.Context passed to Stop
	onStart func(name string)
	onStop  func(name string)
}

func (p *probeRunner) Name() string { return p.name }

func (p *probeRunner) Start(ctx context.Context) error {
	p.started.Add(1)
	if p.onStart != nil {
		p.onStart(p.name)
	}
	if p.start != nil {
		return p.start(ctx)
	}
	<-ctx.Done()
	return nil
}

func (p *probeRunner) Stop(ctx context.Context) error {
	p.stopped.Add(1)
	p.stopCtx.Store(ctx)
	if p.onStop != nil {
		p.onStop(p.name)
	}
	if p.stop != nil {
		return p.stop(ctx)
	}
	return nil
}

// recorder collects event names in order.
type recorder struct {
	mu     sync.Mutex
	events []string
}

func (r *recorder) add(e string) {
	r.mu.Lock()
	r.events = append(r.events, e)
	r.mu.Unlock()
}

func (r *recorder) get() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.events...)
}

func quietLane(timeout time.Duration) *Lane {
	return New(WithLogger(slog.New(slog.NewTextHandler(io.Discard, nil))), WithShutdownTimeout(timeout))
}

func runLane(t *testing.T, l *Lane, ctx context.Context) error {
	t.Helper()
	done := make(chan error, 1)
	go func() { done <- l.Run(ctx) }()
	select {
	case err := <-done:
		return err
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return")
		return nil
	}
}

// #28: every runner is started (concurrently, no ordering) and stopped
// exactly once, in reverse registration order, for any number of runners.
func TestLifecycle_OrderingGuarantees(t *testing.T) {
	for _, n := range []int{1, 2, 5, 20} {
		l := quietLane(time.Second)
		var rec recorder
		var allStarted sync.WaitGroup
		allStarted.Add(n)
		runners := make([]*probeRunner, n)
		for i := range n {
			runners[i] = &probeRunner{
				name:    string(rune('a' + i)),
				onStart: func(string) { allStarted.Done() },
				onStop:  rec.add,
			}
			l.AddRunner(runners[i])
		}

		ctx, cancel := context.WithCancel(context.Background())
		go func() { allStarted.Wait(); cancel() }()
		if err := runLane(t, l, ctx); err != nil {
			t.Fatalf("n=%d: %v", n, err)
		}

		got := rec.get()
		for i, r := range runners {
			if r.started.Load() != 1 || r.stopped.Load() != 1 {
				t.Fatalf("n=%d %s: started %d stopped %d", n, r.name, r.started.Load(), r.stopped.Load())
			}
			if got[i] != runners[n-1-i].name {
				t.Fatalf("n=%d: stop order %v is not reverse registration order", n, got)
			}
		}
	}
}

// #29: with some runners failing to stop, all still receive Stop in order and
// all failures are reported.
func TestLifecycle_PartialStopFailures(t *testing.T) {
	l := quietLane(time.Second)
	var rec recorder
	errB := errors.New("b failed")
	for _, name := range []string{"a", "b", "c", "d", "e"} {
		r := &probeRunner{name: name, onStop: rec.add}
		switch name {
		case "b":
			r.stop = func(context.Context) error { return errB }
		case "d":
			r.stop = func(context.Context) error { panic("d exploded") }
		}
		l.AddRunner(r)
	}

	ctx, cancel := context.WithCancel(context.Background())
	time.AfterFunc(20*time.Millisecond, cancel)
	err := runLane(t, l, ctx)

	if !errors.Is(err, errB) {
		t.Fatalf("missing b stop error: %v", err)
	}
	var pe *PanicError
	if !errors.As(err, &pe) || pe.Runner != "d" || pe.Op != "stop" {
		t.Fatalf("missing d stop panic: %v", err)
	}
	if got := rec.get(); len(got) != 5 || got[0] != "e" || got[4] != "a" {
		t.Fatalf("stop order %v", got)
	}
	if l.Health().State() != StateStopped {
		t.Fatalf("final state %v", l.Health().State())
	}
}

// #30: several runners fail at startup at the same moment; Run returns one
// of their errors, every runner is stopped, Lane never becomes ready.
func TestLifecycle_ConcurrentStartupFailures(t *testing.T) {
	l := quietLane(time.Second)
	gate := make(chan struct{})
	errs := map[string]error{}
	var runners []*probeRunner
	for _, name := range []string{"a", "b", "c", "d"} {
		e := errors.New(name + " failed")
		errs[name] = e
		r := &probeRunner{name: name, start: func(context.Context) error { <-gate; return e }}
		runners = append(runners, r)
		l.AddRunner(r)
	}
	ok := &probeRunner{name: "ok"}
	l.AddRunner(ok)

	time.AfterFunc(20*time.Millisecond, func() { close(gate) })
	err := runLane(t, l, context.Background())

	matched := 0
	for _, e := range errs {
		if errors.Is(err, e) {
			matched++
		}
	}
	if matched != 1 {
		t.Fatalf("expected exactly one primary failure, matched %d: %v", matched, err)
	}
	for _, r := range append(runners, ok) {
		if r.stopped.Load() != 1 {
			t.Fatalf("%s stopped %d times", r.name, r.stopped.Load())
		}
	}
	if l.Health().State() != StateStopped {
		t.Fatalf("final state %v", l.Health().State())
	}
}

// #31: ctx already cancelled before Run → clean, never ready, all stopped.
func TestLifecycle_CancelledBeforeRun(t *testing.T) {
	l := quietLane(time.Second)
	rd := newReadyFake("http", new([]string), new(sync.Mutex))
	p := &probeRunner{name: "p"}
	l.AddRunner(rd, p)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	var everReady atomic.Bool
	stopPoll := make(chan struct{})
	go func() {
		for {
			select {
			case <-stopPoll:
				return
			default:
				if l.Health().Ready() {
					everReady.Store(true)
				}
			}
		}
	}()
	err := runLane(t, l, ctx)
	close(stopPoll)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if everReady.Load() {
		t.Fatal("became ready despite cancelled ctx")
	}
	if p.stopped.Load() != 1 {
		t.Fatal("runner not stopped")
	}
}

// #31: cancellation while one runner is ready and another is still starting.
func TestLifecycle_CancelWhileSomeRunnersStarting(t *testing.T) {
	l := quietLane(time.Second)
	slow := newReadyFake("slow", new([]string), new(sync.Mutex)) // never ready
	fast := newReadyFake("fast", new([]string), new(sync.Mutex))
	close(fast.ready)
	l.AddRunner(fast, slow)

	ctx, cancel := context.WithCancel(context.Background())
	time.AfterFunc(30*time.Millisecond, cancel)
	if err := runLane(t, l, ctx); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if l.Health().State() != StateStopped {
		t.Fatalf("final state %v", l.Health().State())
	}
}

// #32: Stop receives the shutdown context with the configured deadline, and
// cancelling the parent ctx again during shutdown does not cut it short.
func TestLifecycle_ShutdownContextPropagation(t *testing.T) {
	l := quietLane(500 * time.Millisecond)
	stopping := make(chan struct{})
	parentDuringStop := make(chan error, 1)
	p := &probeRunner{name: "p", stop: func(ctx context.Context) error {
		close(stopping)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(50 * time.Millisecond):
			return nil // finishes well before the deadline
		}
	}}
	l.AddRunner(p)

	ctx, cancel := context.WithCancel(context.Background())
	time.AfterFunc(20*time.Millisecond, cancel)
	go func() { <-stopping; parentDuringStop <- ctx.Err() }()
	err := runLane(t, l, ctx)

	if err != nil {
		t.Fatalf("shutdown should succeed within its own deadline: %v", err)
	}
	if <-parentDuringStop == nil {
		t.Fatal("parent ctx should already be cancelled during Stop")
	}
	if _, ok := p.stopCtx.Load().(context.Context).Deadline(); !ok {
		t.Fatal("Stop ctx has no deadline")
	}
}

// #32: a stop error and a stop timeout in the same shutdown are both
// reported; later runners are still stopped; Run returns within the bound.
func TestLifecycle_StopErrorAndTimeoutCombined(t *testing.T) {
	l := quietLane(100 * time.Millisecond)
	var rec recorder
	errA := errors.New("a failed")
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })

	a := &probeRunner{name: "a", onStop: rec.add, stop: func(context.Context) error { return errA }}
	b := &probeRunner{name: "b", onStop: rec.add, stop: func(context.Context) error { <-release; return nil }} // ignores ctx
	c := &probeRunner{name: "c", onStop: rec.add}
	l.AddRunner(a, b, c)

	ctx, cancel := context.WithCancel(context.Background())
	time.AfterFunc(20*time.Millisecond, cancel)
	start := time.Now()
	err := runLane(t, l, ctx)

	if d := time.Since(start); d > time.Second {
		t.Fatalf("shutdown took %v", d)
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("missing timeout: %v", err)
	}
	// a is reached after the deadline: it still gets Stop but its result is
	// only logged, per the shutdown contract.
	deadline := time.Now().Add(time.Second)
	for len(rec.get()) < 3 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if got := rec.get(); len(got) != 3 || got[0] != "c" || got[1] != "b" || got[2] != "a" {
		t.Fatalf("stop order %v", got)
	}
	if l.Health().State() != StateStopped {
		t.Fatalf("final state %v", l.Health().State())
	}
}
