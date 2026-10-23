package runners

import (
	"context"
	"fmt"
	"log/slog"
	"runtime/debug"
	"time"
)

// Restartable is the runner a Supervisor wraps. It matches lane.Runner;
// runners does not import the core package. Its Start MUST support being
// called again after it returned.
type Restartable interface {
	Name() string
	Start(ctx context.Context) error
	Stop(ctx context.Context) error
}

// Supervisor restarts a runner whose Start fails, with backoff, instead of
// letting the failure shut the whole Lane down.
//
//   - Start returns nil (finished) or ctx is cancelled → Supervisor returns, no
//     restart.
//   - Start returns an error or panics → the error handler is called (default:
//     log), then Start is called again after a backoff (WithBackoff,
//     WithBackoffJitter; default 100ms doubling to 30s). An attempt that ran
//     longer than the backoff cap resets the backoff and the restart count.
//   - After WithMaxRestarts consecutive restarts (default: unlimited) the last
//     error is returned, which Lane treats as a runner failure.
//   - The backoff wait ends immediately on cancellation.
//
// The wrapped runner's Stop is called only once, by Lane at shutdown, not
// between attempts: Start must release its own resources before returning.
// If the wrapped runner is a Readier, WaitReady is forwarded to it for the
// first attempt; Lane's readiness does not change while a runner restarts.
//
// Only wrap runners whose Start can run more than once: WorkerRunner,
// SchedulerRunner and FuncRunner qualify; HTTPRunner and TCPRunner do not.
type Supervisor struct {
	inner       Restartable
	log         *slog.Logger
	onError     func(err error)
	baseBackoff time.Duration
	maxBackoff  time.Duration
	jitter      bool
	maxRestarts int
}

// NewSupervisor wraps r. Applicable options: WithLogger, WithErrorHandler,
// WithBackoff, WithBackoffJitter, WithMaxRestarts.
func NewSupervisor(r Restartable, opts ...Option) *Supervisor {
	o := newOptions(opts)
	s := &Supervisor{
		inner:       r,
		log:         o.log,
		onError:     o.onError,
		baseBackoff: o.baseBackoff,
		maxBackoff:  o.maxBackoff,
		jitter:      o.jitter,
		maxRestarts: o.maxRestarts,
	}
	if s.onError == nil {
		s.onError = func(err error) {
			s.log.Error("supervised runner failed; restarting", "runner", r.Name(), "error", err)
		}
	}
	return s
}

// Name returns the wrapped runner's name.
func (s *Supervisor) Name() string { return s.inner.Name() }

func (s *Supervisor) Start(ctx context.Context) error {
	backoff, restarts := s.baseBackoff, 0
	for {
		began := time.Now()
		err := s.attempt(ctx)
		if err == nil || ctx.Err() != nil {
			return err
		}
		if time.Since(began) > s.maxBackoff {
			backoff, restarts = s.baseBackoff, 0 // it ran healthily for a while
		}
		if s.maxRestarts > 0 && restarts >= s.maxRestarts {
			return fmt.Errorf("%s: giving up after %d restarts: %w", s.Name(), restarts, err)
		}
		s.onError(err)

		timer := time.NewTimer(jittered(backoff, s.jitter))
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil
		case <-timer.C:
		}
		backoff = min(backoff*2, s.maxBackoff)
		restarts++
	}
}

// attempt runs one Start, converting a panic into an error.
func (s *Supervisor) attempt(ctx context.Context) (err error) {
	defer func() {
		if rec := recover(); rec != nil {
			s.log.Error("supervised runner panic", "runner", s.Name(),
				"panic", fmt.Sprintf("%v", rec), "stack", string(debug.Stack()))
			err = fmt.Errorf("%s panicked: %v", s.Name(), rec)
		}
	}()
	return s.inner.Start(ctx)
}

// WaitReady forwards to the wrapped runner if it implements WaitReady;
// otherwise it returns nil immediately.
func (s *Supervisor) WaitReady(ctx context.Context) error {
	if rd, ok := s.inner.(interface {
		WaitReady(ctx context.Context) error
	}); ok {
		return rd.WaitReady(ctx)
	}
	return nil
}

// Stop forwards to the wrapped runner.
func (s *Supervisor) Stop(ctx context.Context) error { return s.inner.Stop(ctx) }
