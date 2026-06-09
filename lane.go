package lane

import (
	"context"
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
// Returns the first runner error, or nil on clean shutdown.
func (l *Lane) Run(ctx context.Context) error {
	sigCh, stopSignals := setupSignals()
	defer stopSignals()

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	g, gctx := errgroup.WithContext(ctx)

	for _, r := range l.runners {
		r := r
		g.Go(func() error {
			if err := r.Start(gctx); err != nil {
				l.log.Error("runner failed", "runner", r.Name(), "error", err)
				return err
			}
			return nil
		})
	}

	l.health.SetReady(true)

	select {
	case <-sigCh:
	case <-gctx.Done():
		l.log.Warn("runner context cancelled, initiating shutdown")
	}

	l.health.SetReady(false)
	cancel()

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), l.shutdownTimeout)
	defer shutdownCancel()

	for i := len(l.runners) - 1; i >= 0; i-- {
		r := l.runners[i]
		if err := r.Stop(shutdownCtx); err != nil {
			l.log.Error("runner stop error", "runner", r.Name(), "error", err)
		}
	}

	return g.Wait()
}
