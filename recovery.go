package lane

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"runtime"
	"runtime/debug"
)

// PanicError is returned by Lane.Run when a Runner's Start, Ready, or Stop
// panics. Lane recovers the panic, logs the stack trace, and treats it like an
// error returned from that method. Use errors.As to inspect it.
type PanicError struct {
	Runner string // Runner name
	Op     string // "start", "ready" or "stop"
	Value  any    // value passed to panic
	Stack  []byte // stack trace of the panicking goroutine
}

func (e *PanicError) Error() string {
	return fmt.Sprintf("runner %s panicked during %s: %v", e.Runner, e.Op, e.Value)
}

// callRunner runs fn, converting a panic into a *PanicError.
func callRunner(log *slog.Logger, name, op string, fn func() error) (err error) {
	defer func() {
		if rec := recover(); rec != nil {
			pe := &PanicError{Runner: name, Op: op, Value: rec, Stack: debug.Stack()}
			log.Error("runner panic",
				"runner", name,
				"op", op,
				"panic", fmt.Sprintf("%v", rec),
				"stack", string(pe.Stack),
			)
			err = pe
		}
	}()
	return fn()
}

// Go runs fn in a goroutine with structured panic recovery.
// On panic: logs stack trace and calls cancel to propagate failure up.
// Use this for goroutines that are critical to service operation.
func Go(ctx context.Context, log *slog.Logger, name string, cancel context.CancelFunc, fn func(ctx context.Context)) {
	go func() {
		defer recoverPanic(log, name, cancel)
		fn(ctx)
	}()
}

// RecoverMiddleware wraps HTTP handlers with panic recovery.
// On panic: logs structured context and returns 500. Does NOT cancel app context —
// a single handler panic is recoverable; the service continues.
func RecoverMiddleware(log *slog.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			defer func() {
				if rec := recover(); rec != nil {
					buf := make([]byte, 4096)
					n := runtime.Stack(buf, false)
					log.Error("http handler panic",
						"panic", fmt.Sprintf("%v", rec),
						"method", r.Method,
						"path", r.URL.Path,
						"stack", string(buf[:n]),
					)
					http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
				}
			}()
			next.ServeHTTP(w, r)
		})
	}
}

func recoverPanic(log *slog.Logger, name string, cancel context.CancelFunc) {
	if rec := recover(); rec != nil {
		buf := make([]byte, 4096)
		n := runtime.Stack(buf, false)
		log.Error("goroutine panic",
			"goroutine", name,
			"panic", fmt.Sprintf("%v", rec),
			"stack", string(buf[:n]),
		)
		cancel()
	}
}
