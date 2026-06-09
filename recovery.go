package lane

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"runtime"
)

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
