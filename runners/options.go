package runners

import (
	"context"
	"log/slog"
	"time"
)

// Func is the unit of work run by WorkerRunner (each iteration) and
// SchedulerRunner (each tick).
type Func func(ctx context.Context) error

// Option configures a built-in runner. Options shared by all runners
// (WithLogger) apply everywhere; runner-specific options are ignored by
// runners they don't apply to.
type Option func(*options)

type options struct {
	log     *slog.Logger
	onError func(err error)

	// WorkerRunner
	baseBackoff time.Duration
	maxBackoff  time.Duration
	jitter      bool

	// SchedulerRunner
	immediate bool

	// HTTPRunner
	certFile, keyFile string
}

func newOptions(opts []Option) options {
	o := options{
		log:         slog.Default(),
		baseBackoff: defaultWorkerBaseBackoff,
		maxBackoff:  defaultWorkerMaxBackoff,
	}
	for _, opt := range opts {
		opt(&o)
	}
	return o
}

// WithLogger sets the runner's logger (default slog.Default()). nil is ignored.
func WithLogger(log *slog.Logger) Option {
	return func(o *options) {
		if log != nil {
			o.log = log
		}
	}
}

// WithErrorHandler overrides the default log-and-continue handler for errors
// returned by a WorkerRunner or SchedulerRunner Func. The runner keeps running
// after the handler returns.
func WithErrorHandler(fn func(err error)) Option {
	return func(o *options) { o.onError = fn }
}

// WithBackoff sets WorkerRunner's delay after the first consecutive failure
// (initial) and the cap it doubles up to (limit). Values <= 0 keep the
// default; limit is raised to initial if smaller.
func WithBackoff(initial, limit time.Duration) Option {
	return func(o *options) {
		if initial > 0 {
			o.baseBackoff = initial
		}
		if limit > 0 {
			o.maxBackoff = limit
		}
		o.maxBackoff = max(o.maxBackoff, o.baseBackoff)
	}
}

// WithBackoffJitter randomizes each WorkerRunner backoff delay to between half
// and the full computed delay, spreading out retries from many workers
// failing together. Without it, delays are deterministic.
func WithBackoffJitter() Option {
	return func(o *options) { o.jitter = true }
}

// WithImmediateRun makes SchedulerRunner run its Func once as soon as Start is
// called, then on every tick. The interval is still measured from Start.
func WithImmediateRun() Option {
	return func(o *options) { o.immediate = true }
}

// WithTLS makes HTTPRunner serve TLS using the given certificate and key files.
func WithTLS(certFile, keyFile string) Option {
	return func(o *options) { o.certFile, o.keyFile = certFile, keyFile }
}
