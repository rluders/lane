package lane

import (
	"context"
	"errors"
	"slices"
	"sync"
	"testing"
	"time"
)

// gatedReadier is a probeRunner that is ready once gate is closed.
type gatedReadier struct {
	*probeRunner
	gate chan struct{}
}

func newGated(name string, onStop func(string)) *gatedReadier {
	return &gatedReadier{probeRunner: &probeRunner{name: name, onStop: onStop}, gate: make(chan struct{})}
}

func (g *gatedReadier) WaitReady(ctx context.Context) error {
	select {
	case <-g.gate:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func eventuallyTrue(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(2 * time.Millisecond)
	}
}

func TestStages_LaterStageWaitsForEarlierReadiness(t *testing.T) {
	var log eventLog
	var rec recorder
	l := observedLane(&log)
	db := newGated("db", rec.add)
	cache := &probeRunner{name: "cache", onStop: rec.add} // not a Readier
	api := newGated("api", rec.add)
	worker := &probeRunner{name: "worker", onStop: rec.add}
	l.AddStage(db, cache)
	l.AddStage(api, worker)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- l.Run(ctx) }()

	eventuallyTrue(t, "stage 1 started", func() bool { return db.started.Load() == 1 && cache.started.Load() == 1 })
	time.Sleep(30 * time.Millisecond)
	if api.started.Load() != 0 || worker.started.Load() != 0 {
		t.Fatal("stage 2 started before stage 1 was ready")
	}

	close(db.gate)
	eventuallyTrue(t, "stage 2 started", func() bool { return api.started.Load() == 1 && worker.started.Load() == 1 })
	if l.Health().Ready() {
		t.Fatal("ready before stage 2 was ready")
	}

	close(api.gate)
	waitReady(t, l)
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}

	if got := rec.get(); !slices.Equal(got, []string{"worker", "api", "cache", "db"}) {
		t.Fatalf("stop order %v, want reverse stages, LIFO within", got)
	}
	if log.index(EventRunnerReady, "db") > log.index(EventRunnerStarting, "api") {
		t.Fatalf("stage 2 starting before stage 1 ready: %v", kinds(log.all()))
	}
}

func TestStages_StartupFailureSkipsLaterStages(t *testing.T) {
	for name, setup := range map[string]func(first *gatedReadier){
		"start error": func(first *gatedReadier) {
			first.start = func(context.Context) error { return errors.New("db down") }
		},
		"panic": func(first *gatedReadier) {
			first.start = func(context.Context) error { panic("db exploded") }
		},
	} {
		t.Run(name, func(t *testing.T) {
			var log eventLog
			l := observedLane(&log)
			db := newGated("db", nil)
			setup(db)
			api := &probeRunner{name: "api"}
			l.AddStage(db)
			l.AddStage(api)

			if err := l.Run(context.Background()); err == nil {
				t.Fatal("expected startup error")
			}
			if api.started.Load() != 0 || api.stopped.Load() != 0 {
				t.Fatalf("later stage touched: started %d stopped %d", api.started.Load(), api.stopped.Load())
			}
			if db.stopped.Load() != 1 {
				t.Fatal("started runner not stopped")
			}
			if log.index(EventRunnerStarting, "api") >= 0 {
				t.Fatal("RunnerStarting emitted for a stage that never started")
			}
		})
	}
}

type failingReadier struct{ *probeRunner }

func (f failingReadier) WaitReady(context.Context) error { return errors.New("migration failed") }

func TestStages_ReadinessErrorSkipsLaterStages(t *testing.T) {
	l := quietLane(time.Second)
	db := failingReadier{&probeRunner{name: "db"}}
	api := &probeRunner{name: "api"}
	l.AddStage(db)
	l.AddStage(api)

	err := l.Run(context.Background())
	if err == nil || api.started.Load() != 0 {
		t.Fatalf("err=%v api started=%d", err, api.started.Load())
	}
}

func TestStages_CancelWhileWaitingSkipsLaterStages(t *testing.T) {
	l := quietLane(time.Second)
	db := newGated("db", nil) // never ready
	api := &probeRunner{name: "api"}
	l.AddStage(db)
	l.AddStage(api)

	ctx, cancel := context.WithCancel(context.Background())
	time.AfterFunc(20*time.Millisecond, cancel)
	if err := l.Run(ctx); err != nil {
		t.Fatalf("cancellation should be clean: %v", err)
	}
	if api.started.Load() != 0 || api.stopped.Load() != 0 {
		t.Fatal("later stage touched after cancellation")
	}
	if db.stopped.Load() != 1 {
		t.Fatal("first stage not stopped")
	}
}

func TestStages_NonReadierStageAdvancesImmediately(t *testing.T) {
	l := quietLane(time.Second)
	var wg sync.WaitGroup
	wg.Add(3)
	for _, n := range []string{"a", "b", "c"} {
		l.AddStage(&probeRunner{name: n, onStart: func(string) { wg.Done() }})
	}
	ctx, cancel := context.WithCancel(context.Background())
	go func() { wg.Wait(); cancel() }()
	if err := runLane(t, l, ctx); err != nil {
		t.Fatal(err)
	}
}

func TestStages_Registration(t *testing.T) {
	l := quietLane(time.Second)
	a, b, c, d := &probeRunner{name: "a"}, &probeRunner{name: "b"}, &probeRunner{name: "c"}, &probeRunner{name: "d"}
	l.AddRunner(a)    // implicit first stage
	l.AddStage()      // ignored
	l.AddStage(b)     // second stage
	l.AddRunner(c, d) // joins the second stage
	if len(l.stages) != 2 || len(l.stages[0]) != 1 || len(l.stages[1]) != 3 {
		t.Fatalf("stages: %v", l.stages)
	}
	if got := l.runners(); len(got) != 4 || got[0] != Runner(a) || got[3] != Runner(d) {
		t.Fatalf("runners: %v", got)
	}
	l.AddRunner() // no-op
	if len(l.stages) != 2 {
		t.Fatal("empty AddRunner changed stages")
	}
}
