package lane

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"sync/atomic"
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

// syncBuffer is a goroutine-safe log sink.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func TestGo_PanicLogsFullStack(t *testing.T) {
	var out syncBuffer
	log := slog.New(slog.NewJSONHandler(&out, nil))
	ctx, cancel := context.WithCancel(context.Background())

	Go(ctx, log, "deep", cancel, func(ctx context.Context) {
		deepPanic(200)
	})
	<-ctx.Done()

	var entry struct {
		Msg       string `json:"msg"`
		Goroutine string `json:"goroutine"`
		Panic     string `json:"panic"`
		Stack     string `json:"stack"`
	}
	if err := json.Unmarshal([]byte(out.String()), &entry); err != nil {
		t.Fatalf("decode log: %v\n%s", err, out.String())
	}
	if entry.Goroutine != "deep" || entry.Panic != "bottom" {
		t.Fatalf("unexpected log entry: %+v", entry)
	}
	// A 200-frame stack is far beyond the old 4KB truncation.
	if len(entry.Stack) <= 4096 || !strings.Contains(entry.Stack, "TestGo_PanicLogsFullStack") {
		t.Fatalf("stack trace truncated or missing caller (%d bytes)", len(entry.Stack))
	}
}

func deepPanic(n int) {
	if n == 0 {
		panic("bottom")
	}
	deepPanic(n - 1)
}

// spawningRunner starts a critical background goroutine via Go, wired to
// cancel lane's context, then blocks like a normal runner.
type spawningRunner struct {
	log     *slog.Logger
	cancel  context.CancelFunc
	trigger chan struct{}
	stopped atomic.Bool
}

func (s *spawningRunner) Name() string { return "spawner" }

func (s *spawningRunner) Start(ctx context.Context) error {
	Go(ctx, s.log, "background", s.cancel, func(ctx context.Context) {
		<-s.trigger
		panic("background failure")
	})
	<-ctx.Done()
	return nil
}

func (s *spawningRunner) Stop(context.Context) error {
	s.stopped.Store(true)
	return nil
}

func TestGo_PanicTriggersLaneShutdown(t *testing.T) {
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	l := New(WithLogger(log), WithShutdownTimeout(time.Second))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	r := &spawningRunner{log: log, cancel: cancel, trigger: make(chan struct{})}
	l.AddRunner(r)

	done := make(chan error, 1)
	go func() { done <- l.Run(ctx) }()

	deadline := time.Now().Add(time.Second)
	for !l.Health().Ready() {
		if time.Now().After(deadline) {
			t.Fatal("lane did not become ready")
		}
		time.Sleep(5 * time.Millisecond)
	}
	close(r.trigger)

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run returned %v; Go panics are reported in logs, not Run's error", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("lane did not shut down after background panic")
	}
	if !r.stopped.Load() {
		t.Fatal("runner was not stopped")
	}
	if l.Health().Ready() {
		t.Fatal("still ready after shutdown")
	}
}
