package lane

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// eventLog is a test observer that records every event.
type eventLog struct {
	mu     sync.Mutex
	events []Event
}

func (e *eventLog) observe(ev Event) {
	e.mu.Lock()
	e.events = append(e.events, ev)
	e.mu.Unlock()
}

func (e *eventLog) all() []Event {
	e.mu.Lock()
	defer e.mu.Unlock()
	return slices.Clone(e.events)
}

func (e *eventLog) index(kind EventKind, runner string) int {
	return slices.IndexFunc(e.all(), func(ev Event) bool { return ev.Kind == kind && ev.Runner == runner })
}

func (e *eventLog) find(kind EventKind, runner string) Event {
	evs := e.all()
	if i := slices.IndexFunc(evs, func(ev Event) bool { return ev.Kind == kind && ev.Runner == runner }); i >= 0 {
		return evs[i]
	}
	return Event{Kind: -1}
}

func observedLane(log *eventLog, opts ...Option) *Lane {
	base := []Option{
		WithLogger(slog.New(slog.NewTextHandler(io.Discard, nil))),
		WithShutdownTimeout(time.Second),
		WithObserver(log.observe),
	}
	return New(append(base, opts...)...)
}

func TestEvents_CleanLifecycleSequence(t *testing.T) {
	var log eventLog
	l := observedLane(&log)
	rd := newReadyFake("http", new([]string), new(sync.Mutex))
	plain := &probeRunner{name: "worker"}
	l.AddRunner(plain, rd)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- l.Run(ctx) }()
	time.Sleep(20 * time.Millisecond)
	close(rd.ready)
	waitReady(t, l)
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}

	evs := log.all()
	if evs[0].Kind != EventLaneStarting || evs[len(evs)-1].Kind != EventLaneStopped {
		t.Fatalf("lane events must bracket the run: %v", kinds(evs))
	}
	if evs[len(evs)-1].Err != nil || evs[len(evs)-1].Duration <= 0 {
		t.Fatalf("LaneStopped: %+v", evs[len(evs)-1])
	}
	before := func(a, b int, what string) {
		t.Helper()
		if a < 0 || b < 0 || a >= b {
			t.Fatalf("%s out of order (%d, %d): %v", what, a, b, kinds(evs))
		}
	}
	before(log.index(EventRunnerStarting, "worker"), log.index(EventRunnerStarting, "http"), "starting in registration order")
	before(log.index(EventRunnerReady, "http"), log.index(EventLaneReady, ""), "runner ready before lane ready")
	before(log.index(EventRunnerReady, "worker"), log.index(EventLaneReady, ""), "runner ready before lane ready")
	before(log.index(EventLaneReady, ""), log.index(EventLaneStopping, ""), "ready before stopping")
	before(log.index(EventLaneStopping, ""), log.index(EventRunnerStopping, "http"), "lane stopping before runner stopping")
	before(log.index(EventRunnerStopped, "http"), log.index(EventRunnerStopping, "worker"), "LIFO stop")

	if e := log.find(EventLaneStopping, ""); e.Err != nil {
		t.Fatalf("intentional shutdown should have nil cause: %v", e.Err)
	}
	for _, name := range []string{"worker", "http"} {
		if e := log.find(EventRunnerExited, name); e.Kind != EventRunnerExited || e.Err != nil {
			t.Fatalf("%s exited: %+v", name, e)
		}
		if e := log.find(EventRunnerStopped, name); e.Err != nil {
			t.Fatalf("%s stopped: %+v", name, e)
		}
	}
	if d := log.find(EventLaneReady, "").Duration; d < 20*time.Millisecond {
		t.Fatalf("LaneReady duration %v should include the readiness wait", d)
	}
}

func TestEvents_FailureAndStopError(t *testing.T) {
	var log eventLog
	l := observedLane(&log)
	errStart, errStop := errors.New("db down"), errors.New("flush failed")
	failing := &probeRunner{name: "db", start: func(context.Context) error { return errStart }}
	// A Readier that never becomes ready keeps Lane in Starting, so the
	// failure is a startup failure (without one, Lane is ready on launch).
	other := newReadyFake("api", new([]string), new(sync.Mutex))
	other.stopErr = errStop
	l.AddRunner(failing, other)

	err := l.Run(context.Background())
	if !errors.Is(err, errStart) || !errors.Is(err, errStop) {
		t.Fatalf("Run: %v", err)
	}
	if log.index(EventLaneReady, "") >= 0 {
		t.Fatal("LaneReady emitted despite startup failure")
	}
	if e := log.find(EventLaneStopping, ""); !errors.Is(e.Err, errStart) {
		t.Fatalf("LaneStopping cause: %v", e.Err)
	}
	if e := log.find(EventRunnerExited, "db"); !errors.Is(e.Err, errStart) {
		t.Fatalf("db exit: %v", e.Err)
	}
	if e := log.find(EventRunnerStopped, "api"); !errors.Is(e.Err, errStop) {
		t.Fatalf("api stop: %v", e.Err)
	}
	if e := log.all()[len(log.all())-1]; e.Kind != EventLaneStopped || !errors.Is(e.Err, errStart) {
		t.Fatalf("LaneStopped: %+v", e)
	}
}

func TestEvents_ObserversSerializedOrderedAndPanicSafe(t *testing.T) {
	var inFlight, overlaps atomic.Int32
	var order []string
	var mu sync.Mutex
	record := func(name string) Observer {
		return func(ev Event) {
			if inFlight.Add(1) > 1 {
				overlaps.Add(1)
			}
			defer inFlight.Add(-1)
			if ev.Kind == EventLaneStarting {
				mu.Lock()
				order = append(order, name)
				mu.Unlock()
			}
		}
	}
	panicky := func(Event) { panic("observer bug") }

	l := New(
		WithLogger(slog.New(slog.NewTextHandler(io.Discard, nil))),
		WithObserver(record("first")),
		WithObserver(panicky),
		WithObserver(record("second")),
	)
	for i := range 10 {
		l.AddRunner(&probeRunner{name: string(rune('a' + i))})
	}
	ctx, cancel := context.WithCancel(context.Background())
	time.AfterFunc(30*time.Millisecond, cancel)
	if err := l.Run(ctx); err != nil {
		t.Fatalf("observer panic leaked into Run: %v", err)
	}
	if overlaps.Load() != 0 {
		t.Fatalf("observers called concurrently %d times", overlaps.Load())
	}
	if !slices.Equal(order, []string{"first", "second"}) {
		t.Fatalf("observer order %v", order)
	}
}

func TestEvents_NothingAfterLaneStopped(t *testing.T) {
	var log eventLog
	l := observedLane(&log, WithShutdownTimeout(50*time.Millisecond))
	release := make(chan struct{})
	stuck := &probeRunner{name: "stuck", start: func(context.Context) error { <-release; return nil }}
	l.AddRunner(stuck)

	ctx, cancel := context.WithCancel(context.Background())
	time.AfterFunc(10*time.Millisecond, cancel)
	if err := l.Run(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Run: %v", err)
	}
	close(release) // the abandoned Start now returns
	time.Sleep(30 * time.Millisecond)

	evs := log.all()
	if evs[len(evs)-1].Kind != EventLaneStopped {
		t.Fatalf("event after LaneStopped: %v", kinds(evs))
	}
	if log.index(EventRunnerExited, "stuck") >= 0 {
		t.Fatal("abandoned runner should not report RunnerExited")
	}
}

func TestEvents_NoObserversNoOverhead(t *testing.T) {
	l := New(WithLogger(slog.New(slog.NewTextHandler(io.Discard, nil))), WithObserver(nil))
	if len(l.observers) != 0 {
		t.Fatal("WithObserver(nil) should be ignored")
	}
}

func TestEventKind_String(t *testing.T) {
	if EventRunnerStopped.String() != "runner_stopped" || EventKind(99).String() != "EventKind(99)" {
		t.Fatal("unexpected String output")
	}
}

func kinds(evs []Event) []string {
	out := make([]string, len(evs))
	for i, e := range evs {
		out[i] = e.Kind.String() + ":" + e.Runner
	}
	return out
}
