package lane

import (
	"fmt"
	"log/slog"
	"runtime/debug"
	"sync"
	"time"
)

// EventKind identifies a lifecycle event. See Event.
type EventKind int

const (
	EventLaneStarting   EventKind = iota // Run called
	EventRunnerStarting                  // a runner's Start is about to be called
	EventRunnerReady                     // a runner is ready (WaitReady returned nil, or launched if not a Readier)
	EventLaneReady                       // every runner is ready; Lane is Running
	EventLaneStopping                    // shutdown began; Err is the primary failure, nil for signal/ctx
	EventRunnerStopping                  // a runner's Stop is about to be called
	EventRunnerStopped                   // a runner's Stop returned; Err is its error
	EventRunnerExited                    // a runner's Start returned; Err is non-nil only for a failure
	EventLaneStopped                     // Run is returning; Err is Run's result
)

func (k EventKind) String() string {
	switch k {
	case EventLaneStarting:
		return "lane_starting"
	case EventRunnerStarting:
		return "runner_starting"
	case EventRunnerReady:
		return "runner_ready"
	case EventLaneReady:
		return "lane_ready"
	case EventLaneStopping:
		return "lane_stopping"
	case EventRunnerStopping:
		return "runner_stopping"
	case EventRunnerStopped:
		return "runner_stopped"
	case EventRunnerExited:
		return "runner_exited"
	case EventLaneStopped:
		return "lane_stopped"
	}
	return fmt.Sprintf("EventKind(%d)", int(k))
}

// Event describes one lifecycle transition, delivered to observers registered
// with WithObserver.
type Event struct {
	Kind   EventKind
	Runner string    // runner name; empty for lane-level events
	Err    error     // see EventKind; nil when not applicable
	Time   time.Time // when the event occurred
	// Duration is the elapsed time of the step the event completes:
	// RunnerReady and LaneReady: since LaneStarting; RunnerStopped: the Stop
	// call; RunnerExited: since RunnerStarting; LaneStopped: the whole Run.
	// Zero for other events.
	Duration time.Duration
}

// Observer receives lifecycle events. See WithObserver.
type Observer func(Event)

// WithObserver registers fn to receive lifecycle events. It may be used
// several times; observers are called in registration order.
//
// Delivery is synchronous and serialized: an observer is never called
// concurrently, and events from one Run arrive in the order they happened.
// Per runner the sequence is RunnerStarting, RunnerReady (if it became
// ready), RunnerStopping, RunnerStopped; RunnerExited comes whenever Start
// returns. Lane-level events bracket them: LaneStarting first, LaneStopped
// last. No events are delivered after LaneStopped, so a runner abandoned at
// the shutdown timeout gets no RunnerExited.
//
// Observers run on lifecycle goroutines and must be fast and non-blocking
// (hand off to a channel or goroutine for slow work). A panic in an observer
// is recovered and logged; it never affects the lifecycle.
//
// Observers are the integration point for logging, metrics and tracing; Lane
// itself depends on no observability provider. See docs/observability.md.
func WithObserver(fn Observer) Option {
	return func(l *Lane) {
		if fn != nil {
			l.observers = append(l.observers, fn)
		}
	}
}

// emitter delivers events for one Run.
type emitter struct {
	mu        sync.Mutex
	log       *slog.Logger
	observers []Observer
	closed    bool
}

func (e *emitter) emit(ev Event) {
	if len(e.observers) == 0 {
		return
	}
	if ev.Time.IsZero() {
		ev.Time = time.Now()
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed {
		return
	}
	if ev.Kind == EventLaneStopped {
		e.closed = true
	}
	for _, o := range e.observers {
		e.call(o, ev)
	}
}

func (e *emitter) call(o Observer, ev Event) {
	defer func() {
		if rec := recover(); rec != nil {
			e.log.Error("observer panic",
				"event", ev.Kind.String(),
				"panic", fmt.Sprintf("%v", rec),
				"stack", string(debug.Stack()),
			)
		}
	}()
	o(ev)
}
