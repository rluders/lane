package lane

import (
	"sync"
	"testing"
)

func TestHealth_ZeroValueIsStarting(t *testing.T) {
	h := &Health{}
	if h.State() != StateStarting || !h.Live() || h.Ready() {
		t.Fatalf("zero Health: state=%v live=%v ready=%v", h.State(), h.Live(), h.Ready())
	}
}

func TestHealth_LiveAndReadyPerState(t *testing.T) {
	for _, tt := range []struct {
		state       State
		live, ready bool
	}{
		{StateStarting, true, false},
		{StateRunning, true, true},
		{StateStopping, true, false},
		{StateStopped, false, false},
	} {
		h := &Health{state: tt.state}
		if h.Live() != tt.live || h.Ready() != tt.ready {
			t.Errorf("%v: live=%v ready=%v, want live=%v ready=%v", tt.state, h.Live(), h.Ready(), tt.live, tt.ready)
		}
	}
}

func TestHealth_Transitions(t *testing.T) {
	all := []State{StateStarting, StateRunning, StateStopping, StateStopped}
	valid := map[[2]State]bool{
		{StateStarting, StateRunning}:  true,
		{StateStarting, StateStopping}: true,
		{StateRunning, StateStopping}:  true,
		{StateStopping, StateStopped}:  true,
		{StateStopped, StateStarting}:  true,
	}
	for _, from := range all {
		for _, to := range all {
			h := &Health{state: from}
			ok := h.transition(to)
			want := valid[[2]State{from, to}]
			if ok != want {
				t.Errorf("%v -> %v: got %v, want %v", from, to, ok, want)
			}
			if want && h.State() != to || !want && h.State() != from {
				t.Errorf("%v -> %v: ended in %v", from, to, h.State())
			}
		}
	}
}

func TestState_String(t *testing.T) {
	for s, want := range map[State]string{
		StateStarting: "starting", StateRunning: "running",
		StateStopping: "stopping", StateStopped: "stopped", State(42): "State(42)",
	} {
		if s.String() != want {
			t.Errorf("%d: got %q, want %q", int(s), s.String(), want)
		}
	}
}

func TestHealth_Concurrent(t *testing.T) {
	h := &Health{}
	var wg sync.WaitGroup
	for range 100 {
		wg.Add(2)
		go func() { defer wg.Done(); h.transition(StateRunning) }()
		go func() { defer wg.Done(); _ = h.Ready() && h.Live() }()
	}
	wg.Wait()
}
