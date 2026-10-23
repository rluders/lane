package runners

import (
	"context"
	"log/slog"
	"math/rand/v2"
	"time"
)

const (
	defaultWorkerBaseBackoff = 100 * time.Millisecond
	defaultWorkerMaxBackoff  = 30 * time.Second
)

// WorkerRunner executes a Func in a loop until ctx is cancelled. An error
// from the Func is reported to the error handler and followed by a backoff
// delay; nil runs the next iteration immediately.
//
// After a failed iteration it waits before retrying, starting at 100ms and
// doubling per consecutive failure up to 30s (see WithBackoff and
// WithBackoffJitter). A successful iteration resets the delay. Errors returned after ctx is cancelled are not reported.
// It does NOT stop or restart after failure — if you need that, implement it inside the Func.
type WorkerRunner struct {
	name        string
	fn          Func
	log         *slog.Logger
	onError     func(err error)
	baseBackoff time.Duration
	maxBackoff  time.Duration
	jitter      bool
}

// NewWorkerRunner creates a WorkerRunner. Applicable options: WithLogger,
// WithErrorHandler, WithBackoff, WithBackoffJitter.
func NewWorkerRunner(name string, fn Func, opts ...Option) *WorkerRunner {
	o := newOptions(opts)
	w := &WorkerRunner{
		name:        name,
		fn:          fn,
		log:         o.log,
		onError:     o.onError,
		baseBackoff: o.baseBackoff,
		maxBackoff:  o.maxBackoff,
		jitter:      o.jitter,
	}
	if w.onError == nil {
		w.onError = func(err error) {
			w.log.Error("worker error", "worker", name, "error", err)
		}
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

		timer := time.NewTimer(r.delay(backoff))
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil
		case <-timer.C:
		}
		backoff = min(backoff*2, r.maxBackoff)
	}
}

// delay returns the wait for the given backoff, applying jitter if enabled.
func (r *WorkerRunner) delay(backoff time.Duration) time.Duration {
	return jittered(backoff, r.jitter)
}

// jittered returns backoff, or a random duration in [backoff/2, backoff] if
// jitter is enabled.
func jittered(backoff time.Duration, jitter bool) time.Duration {
	if !jitter {
		return backoff
	}
	half := backoff / 2
	return half + rand.N(backoff-half+1)
}

// Stop is a no-op: ctx cancellation in Start handles shutdown.
func (r *WorkerRunner) Stop(ctx context.Context) error { return nil }
