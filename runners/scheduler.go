package runners

import (
	"context"
	"log/slog"
	"time"
)

// JobFn is a periodic task. Errors are logged and the scheduler continues.
// To stop on error, return a wrapped sentinel and check it in an outer WorkerRunner.
type JobFn func(ctx context.Context) error

// SchedulerRunner executes a JobFn on a fixed interval using time.Ticker.
// Missed ticks (slow jobs) are dropped — no queue buildup.
type SchedulerRunner struct {
	name     string
	interval time.Duration
	fn       JobFn
	log      *slog.Logger
}

func NewSchedulerRunner(name string, interval time.Duration, fn JobFn, log *slog.Logger) *SchedulerRunner {
	return &SchedulerRunner{name: name, interval: interval, fn: fn, log: log}
}

func (r *SchedulerRunner) Name() string { return r.name }

func (r *SchedulerRunner) Start(ctx context.Context) error {
	ticker := time.NewTicker(r.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			if err := r.fn(ctx); err != nil {
				r.log.Error("scheduler job failed", "scheduler", r.name, "error", err)
			}
		}
	}
}

// Stop is a no-op: ctx cancellation in Start handles shutdown.
func (r *SchedulerRunner) Stop(ctx context.Context) error { return nil }
