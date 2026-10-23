package runners

import (
	"context"
	"fmt"
	"log/slog"
	"time"
)

// JobFn is a periodic task. A returned error is reported to the scheduler's
// error handler (by default it is logged) and scheduling continues.
// Retries and backoff for a failing job belong inside JobFn.
type JobFn func(ctx context.Context) error

// SchedulerRunner executes a JobFn on a fixed interval.
//
// Execution semantics:
//   - Fixed rate: runs are aligned to ticks every interval, measured from Start.
//   - The first run happens one interval after Start, or immediately with
//     WithImmediateRun.
//   - Runs never overlap. Ticks that occur while a job is running are dropped,
//     not queued: after a slow job, the next run waits for the next tick.
//   - Cancelling ctx stops scheduling; a running job receives the cancelled ctx
//     and Start returns once it does. No job starts after ctx is cancelled.
//   - A job error never stops the scheduler or affects Lane. Errors returned
//     after ctx is cancelled are not reported. Consecutive failures have no
//     special handling. A panic in a job propagates out of Start, where Lane
//     recovers it and shuts down.
//   - Start returns an error if interval <= 0.
type SchedulerRunner struct {
	name      string
	interval  time.Duration
	fn        JobFn
	log       *slog.Logger
	immediate bool
	onError   func(err error)
}

type SchedulerOption func(*SchedulerRunner)

// WithImmediateRun runs the job once as soon as Start is called, then on every
// tick. The interval is still measured from Start, so the first tick is not
// delayed by the immediate run.
func WithImmediateRun() SchedulerOption {
	return func(s *SchedulerRunner) { s.immediate = true }
}

// WithJobErrorHandler overrides the default log-and-continue job error handler.
// The scheduler keeps running after the handler returns.
func WithJobErrorHandler(fn func(err error)) SchedulerOption {
	return func(s *SchedulerRunner) { s.onError = fn }
}

func NewSchedulerRunner(name string, interval time.Duration, fn JobFn, log *slog.Logger, opts ...SchedulerOption) *SchedulerRunner {
	s := &SchedulerRunner{name: name, interval: interval, fn: fn, log: log}
	s.onError = func(err error) {
		log.Error("scheduler job failed", "scheduler", name, "error", err)
	}
	for _, o := range opts {
		o(s)
	}
	return s
}

func (r *SchedulerRunner) Name() string { return r.name }

func (r *SchedulerRunner) Start(ctx context.Context) error {
	if r.interval <= 0 {
		return fmt.Errorf("scheduler %s: interval must be positive, got %v", r.name, r.interval)
	}

	ticker := time.NewTicker(r.interval)
	defer ticker.Stop()

	if r.immediate {
		r.run(ctx, ticker)
	}

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			r.run(ctx, ticker)
		}
	}
}

// run executes the job once unless ctx is already cancelled, then drops any
// tick that fired while it was running.
func (r *SchedulerRunner) run(ctx context.Context, ticker *time.Ticker) {
	if ctx.Err() != nil {
		return
	}
	if err := r.fn(ctx); err != nil && ctx.Err() == nil {
		r.onError(err)
	}
	select {
	case <-ticker.C:
	default:
	}
}

// Stop is a no-op: ctx cancellation in Start handles shutdown.
func (r *SchedulerRunner) Stop(ctx context.Context) error { return nil }
