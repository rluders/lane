package runners

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"
)

func TestWorkerRunnerBacksOffAfterError(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var calls atomic.Int32
	started := make(chan struct{}, 1)
	runner := NewWorkerRunner("test", func(context.Context) error {
		calls.Add(1)
		select {
		case started <- struct{}{}:
		default:
		}
		return errors.New("failure")
	}, slog.New(slog.NewTextHandler(io.Discard, nil)))

	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = runner.Start(ctx)
	}()

	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("worker did not start")
	}
	time.Sleep(25 * time.Millisecond)
	if got := calls.Load(); got != 1 {
		t.Fatalf("work function called %d times during error backoff; want 1", got)
	}

	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("worker did not stop after context cancellation")
	}
}

func TestWorkerRunnerCancellationInterruptsErrorBackoff(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	var calls atomic.Int32
	runner := NewWorkerRunner("test", func(context.Context) error {
		calls.Add(1)
		return errors.New("failure")
	}, slog.New(slog.NewTextHandler(io.Discard, nil)), WithErrorHandler(func(error) {
		cancel()
	}))

	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = runner.Start(ctx)
	}()

	select {
	case <-done:
	case <-time.After(50 * time.Millisecond):
		t.Fatal("cancellation did not interrupt error backoff")
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("work function called %d times; want 1", got)
	}
}
