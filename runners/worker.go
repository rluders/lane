package runners

import (
	"context"
	"log/slog"
)

// WorkFn is the unit of work executed each iteration.
// Return an error to trigger the onError callback.
// Return ctx.Err() (or nil) to stop cleanly.
type WorkFn func(ctx context.Context) error

// WorkerRunner executes a WorkFn in a tight loop until ctx is cancelled.
// It does NOT restart after failure — if you need retry, implement it inside WorkFn.
type WorkerRunner struct {
	name    string
	fn      WorkFn
	log     *slog.Logger
	onError func(err error)
}

type WorkerOption func(*WorkerRunner)

// WithErrorHandler overrides the default log-and-continue error handler.
// To make errors fatal (stop the worker), call the provided cancel func.
func WithErrorHandler(fn func(err error)) WorkerOption {
	return func(w *WorkerRunner) { w.onError = fn }
}

func NewWorkerRunner(name string, fn WorkFn, log *slog.Logger, opts ...WorkerOption) *WorkerRunner {
	w := &WorkerRunner{
		name: name,
		fn:   fn,
		log:  log,
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
	for {
		select {
		case <-ctx.Done():
			return nil
		default:
			if err := r.fn(ctx); err != nil {
				r.onError(err)
			}
		}
	}
}

// Stop is a no-op: ctx cancellation in Start handles shutdown.
func (r *WorkerRunner) Stop(ctx context.Context) error { return nil }
