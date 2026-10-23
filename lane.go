package lane

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"golang.org/x/sync/errgroup"
)

const defaultShutdownTimeout = 30 * time.Second

// Lane coordinates the lifecycle of one or more Runners. It handles signal
// interception, concurrent startup, health state, and ordered graceful shutdown.
// Dependency wiring is the caller's responsibility.
type Lane struct {
	runners         []Runner
	shutdownTimeout time.Duration
	log             *slog.Logger
	health          *HealthState
}

// Option configures a Lane.
type Option func(*Lane)

// WithShutdownTimeout overrides the default 30s shutdown timeout.
func WithShutdownTimeout(d time.Duration) Option {
	return func(l *Lane) { l.shutdownTimeout = d }
}

// New constructs a Lane. log must not be nil.
func New(log *slog.Logger, opts ...Option) *Lane {
	l := &Lane{
		log:             log,
		shutdownTimeout: defaultShutdownTimeout,
		health:          &HealthState{},
	}
	for _, o := range opts {
		o(l)
	}
	return l
}

// AddRunner appends runners. Registration order determines startup order and
// reverse-order (LIFO) shutdown order.
func (l *Lane) AddRunner(runners ...Runner) {
	l.runners = append(l.runners, runners...)
}

// Health returns the HealthState so callers can pass it to ReadinessHandler.
func (l *Lane) Health() *HealthState {
	return l.health
}

// Run starts all runners concurrently, blocks until a signal or runner failure,
// then shuts down runners in reverse registration order.
//
// Lane becomes ready once every runner implementing Readier reports ready. A
// Ready error is treated like a runner failure. Lane never becomes ready if a
// runner fails or ctx is cancelled first, and is marked not ready as soon as
// shutdown begins.
//
// Every runner receives Stop, even if an earlier Stop failed or the shutdown
// timeout expired; in the latter case runners get an already-expired ctx and
// must return promptly. Stop is called on every runner, including those whose
// Start already returned.
//
// A panic in Start, Ready, or Stop is recovered and treated as an error from
// that method, returned as a *PanicError: in Start or Ready it triggers
// shutdown, in Stop the remaining runners are still stopped. Panics in
// goroutines a runner spawns itself are not recovered; use Go for those.
//
// Returns nil on clean shutdown. Otherwise returns the first runner error
// joined (errors.Join) with every Stop error, in stop order. If several
// runners fail concurrently, only the first error is returned; all are logged.
func (l *Lane) Run(ctx context.Context) error {
	sigCh, stopSignals := setupSignals()
	defer stopSignals()

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	g, gctx := errgroup.WithContext(ctx)

	for _, r := range l.runners {
		r := r
		g.Go(func() error {
			if err := callRunner(l.log, r.Name(), "start", func() error { return r.Start(gctx) }); err != nil {
				l.log.Error("runner failed", "runner", r.Name(), "error", err)
				return err
			}
			return nil
		})
	}

	readyCh := make(chan struct{})
	g.Go(func() error {
		if err := l.waitReady(gctx); err != nil {
			if gctx.Err() != nil {
				return nil
			}
			l.log.Error("runner readiness failed", "error", err)
			return err
		}
		close(readyCh)
		return nil
	})

	// SetReady(true) happens on this goroutine, so it can never race with the
	// SetReady(false) below.
wait:
	for {
		select {
		case <-readyCh:
			readyCh = nil
			if gctx.Err() == nil {
				l.health.SetReady(true)
			}
		case <-sigCh:
			break wait
		case <-gctx.Done():
			l.log.Warn("runner context cancelled, initiating shutdown")
			break wait
		}
	}

	l.health.SetReady(false)
	cancel()

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), l.shutdownTimeout)
	defer shutdownCancel()

	var stopErrs []error
	for i := len(l.runners) - 1; i >= 0; i-- {
		r := l.runners[i]
		if err := callRunner(l.log, r.Name(), "stop", func() error { return r.Stop(shutdownCtx) }); err != nil {
			l.log.Error("runner stop error", "runner", r.Name(), "error", err)
			stopErrs = append(stopErrs, fmt.Errorf("stop %s: %w", r.Name(), err))
		}
	}

	return errors.Join(append([]error{g.Wait()}, stopErrs...)...)
}

// waitReady blocks until every Readier runner is ready, one fails, or ctx is done.
func (l *Lane) waitReady(ctx context.Context) error {
	g, gctx := errgroup.WithContext(ctx)
	for _, r := range l.runners {
		rd, ok := r.(Readier)
		if !ok {
			continue
		}
		name := r.Name()
		g.Go(func() error {
			if err := callRunner(l.log, name, "ready", func() error { return rd.Ready(gctx) }); err != nil {
				return fmt.Errorf("ready %s: %w", name, err)
			}
			return nil
		})
	}
	return g.Wait()
}
