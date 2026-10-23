package runners

import (
	"context"
	"log/slog"
	"time"
)

const (
	defaultWorkerBaseBackoff = 100 * time.Millisecond
	defaultWorkerMaxBackoff  = 30 * time.Second
)

// WorkFn is the unit of work executed each iteration.
// Return an error to trigger the onError callback and a backoff delay.
// Return nil to run the next iteration immediately.
type WorkFn func(ctx context.Context) error

// WorkerRunner executes a WorkFn in a loop until ctx is cancelled.
//
// After a failed iteration it waits before retrying, starting at 100ms and
// doubling per consecutive failure up to 30s. A successful iteration resets
// the delay. Errors returned after ctx is cancelled are not reported.
// It does NOT stop or restart after failure — if you need that, implement it inside WorkFn.
type WorkerRunner struct {
	name        string
	fn          WorkFn
	log         *slog.Logger
	onError     func(err error)
	baseBackoff time.Duration
	maxBackoff  time.Duration
}

type WorkerOption func(*WorkerRunner)

// WithErrorHandler overrides the default log-and-continue error handler.
// The worker keeps running after the handler returns.
func WithErrorHandler(fn func(err error)) WorkerOption {
	return func(w *WorkerRunner) { w.onError = fn }
}

func NewWorkerRunner(name string, fn WorkFn, log *slog.Logger, opts ...WorkerOption) *WorkerRunner {
	w := &WorkerRunner{
		name:        name,
		fn:          fn,
		log:         log,
		baseBackoff: defaultWorkerBaseBackoff,
		maxBackoff:  defaultWorkerMaxBackoff,
	}
	w.onError = func(err error) {
		log.Error("worker error", "worker", name, "error", err)
	}
	for _, o := range opts {
		o(w)
	}
	return w
}

func (r *WorkerRunner) Name() string { return r.name }

func (r *WorkerRunner) Start(ctx context.Context) error {
	backoff := r.baseBackoff
	for {
		if ctx.Err() != nil {
			return nil
		}

		err := r.fn(ctx)
		if err == nil {
			backoff = r.baseBackoff
			continue
		}
		if ctx.Err() != nil {
			return nil
		}
		r.onError(err)

		timer := time.NewTimer(backoff)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil
		case <-timer.C:
		}
		backoff = min(backoff*2, r.maxBackoff)
	}
}

// Stop is a no-op: ctx cancellation in Start handles shutdown.
func (r *WorkerRunner) Stop(ctx context.Context) error { return nil }
