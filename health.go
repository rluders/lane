package lane

import (
	"fmt"
	"sync"
)

// State is the lifecycle state of a Lane, from which liveness and readiness
// are derived.
//
//	Starting ──► Running ──► Stopping ──► Stopped
//	    │                       ▲
//	    └───────────────────────┘  (failure or cancellation during startup)
//
// A Lane is Starting from New until every Readier runner is ready, Running
// until shutdown begins (signal, ctx cancellation, or a runner failure),
// Stopping while runners are stopped, and Stopped once Run returns. Calling
// Run again starts over from Starting.
type State int

const (
	StateStarting State = iota
	StateRunning
	StateStopping
	StateStopped
)

func (s State) String() string {
	switch s {
	case StateStarting:
		return "starting"
	case StateRunning:
		return "running"
	case StateStopping:
		return "stopping"
	case StateStopped:
		return "stopped"
	}
	return fmt.Sprintf("State(%d)", int(s))
}

// validTransitions lists the states each state may move to.
var validTransitions = map[State][]State{
	StateStarting: {StateRunning, StateStopping},
	StateRunning:  {StateStopping},
	StateStopping: {StateStopped},
	StateStopped:  {StateStarting},
}

// Health exposes a Lane's lifecycle state and the liveness and readiness
// derived from it. It is read-only for callers; Lane drives the transitions.
// It has no dependency on HTTP; see LivenessHandler and ReadinessHandler for
// HTTP exposure.
type Health struct {
	mu    sync.RWMutex
	state State
}

// State returns the current lifecycle state.
func (h *Health) State() State {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.state
}

// Live reports whether the Lane is alive: true in every state except Stopped.
// Runner failures do not affect liveness; they move the Lane to Stopping.
func (h *Health) Live() bool { return h.State() != StateStopped }

// Ready reports whether the Lane should receive traffic: true only while
// Running.
func (h *Health) Ready() bool { return h.State() == StateRunning }

// transition moves to next if that is a valid transition from the current
// state and reports whether it did.
func (h *Health) transition(next State) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, s := range validTransitions[h.state] {
		if s == next {
			h.state = next
			return true
		}
	}
	return false
}
