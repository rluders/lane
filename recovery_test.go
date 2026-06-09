package lane

import (
	"context"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"
)

func TestGo_NoPanic(t *testing.T) {
	log := slog.New(slog.NewTextHandler(os.Stderr, nil))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan struct{})
	Go(ctx, log, "test-goroutine", cancel, func(ctx context.Context) {
		defer close(done)
	})
	<-done

	// cancel should NOT have been called by the runtime (no panic)
	select {
	case <-ctx.Done():
		t.Fatal("context was cancelled without a panic")
	default:
	}
}

func TestGo_PanicCancelsContext(t *testing.T) {
	log := slog.New(slog.NewTextHandler(os.Stderr, nil))
	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan struct{})
	Go(ctx, log, "panicking-goroutine", cancel, func(ctx context.Context) {
		defer close(done)
		panic("test panic")
	})
	<-done

	// recoverPanic calls cancel() after fn returns — wait with a short timeout.
	select {
	case <-ctx.Done():
	case <-time.After(100 * time.Millisecond):
		t.Fatal("context was not cancelled after panic")
	}
}

func TestRecoverMiddleware_NoPanic(t *testing.T) {
	log := slog.New(slog.NewTextHandler(os.Stderr, nil))
	handler := RecoverMiddleware(log)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
}

func TestRecoverMiddleware_Panic(t *testing.T) {
	log := slog.New(slog.NewTextHandler(os.Stderr, nil))
	handler := RecoverMiddleware(log)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		panic("handler panic")
	}))

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("expected 500, got %d", rec.Code)
	}
}
