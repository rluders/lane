package lane

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"testing"
	"time"
)

// TestStress_RandomLifecycles runs many short lifecycles with randomized but
// seeded behavior per runner (startup delay/failure, runtime failure, ctx
// errors, panics, slow/failing/stuck Stop) and cancellation at random points.
// Every iteration must return within the shutdown bound, end Stopped, and
// call Stop exactly once on every runner. The seed is logged for reproduction.
func TestStress_RandomLifecycles(t *testing.T) {
	iterations := 120
	if testing.Short() {
		iterations = 20
	}
	seed := uint64(time.Now().UnixNano())
	t.Logf("seed %d", seed)
	rng := rand.New(rand.NewPCG(seed, seed))

	for i := range iterations {
		t.Run(fmt.Sprint(i), func(t *testing.T) {
			stressOnce(t, rand.New(rand.NewPCG(rng.Uint64(), rng.Uint64())))
		})
	}
}

func stressOnce(t *testing.T, rng *rand.Rand) {
	const shutdownTimeout = 50 * time.Millisecond
	l := quietLane(shutdownTimeout)

	ms := func(max int) time.Duration { return time.Duration(rng.IntN(max+1)) * time.Millisecond }
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })

	n := 1 + rng.IntN(6)
	runners := make([]*probeRunner, n)
	for i := range n {
		startDelay, stopDelay := ms(5), ms(5)
		startMode, stopMode := rng.IntN(10), rng.IntN(10)
		r := &probeRunner{name: fmt.Sprintf("r%d", i)}
		r.start = func(ctx context.Context) error {
			select {
			case <-time.After(startDelay):
			case <-ctx.Done():
				return ctx.Err() // idiomatic: report cancellation
			}
			switch startMode {
			case 0:
				return errors.New("start failed")
			case 1:
				panic("start panicked")
			case 2:
				return nil // finishes early
			case 3:
				<-release // ignores cancellation
				return nil
			}
			<-ctx.Done()
			return ctx.Err()
		}
		r.stop = func(ctx context.Context) error {
			select {
			case <-time.After(stopDelay):
			case <-ctx.Done():
				return ctx.Err()
			}
			switch stopMode {
			case 0:
				return errors.New("stop failed")
			case 1:
				panic("stop panicked")
			case 2:
				<-release // ignores its deadline
			}
			return nil
		}
		runners[i] = r
		l.AddRunner(r)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	time.AfterFunc(ms(15), cancel)

	start := time.Now()
	done := make(chan error, 1)
	go func() { done <- l.Run(ctx) }()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Run hung")
	}
	// Bound: cancellation (<=15ms) + startup (<=5ms) + shutdown timeout,
	// with generous slack for the race detector.
	if d := time.Since(start); d > 15*time.Millisecond+shutdownTimeout+500*time.Millisecond {
		t.Fatalf("Run took %v", d)
	}
	if s := l.Health().State(); s != StateStopped {
		t.Fatalf("final state %v", s)
	}
	// Stop is invoked for every runner exactly once (possibly still running
	// in the background if it was reached after the deadline).
	deadline := time.Now().Add(time.Second)
	for _, r := range runners {
		for r.stopped.Load() == 0 && time.Now().Before(deadline) {
			time.Sleep(time.Millisecond)
		}
		if got := r.stopped.Load(); got != 1 {
			t.Fatalf("%s: Stop called %d times", r.name, got)
		}
	}
}
