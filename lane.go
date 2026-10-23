package lane

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"sync"
	"time"

	"golang.org/x/sync/errgroup"
)

const defaultShutdownTimeout = 30 * time.Second

// Lane coordinates the lifecycle of one or more Runners. It handles signal
// interception, concurrent startup, health state, and ordered graceful shutdown.
// Dependency wiring is the caller's responsibility.
type Lane struct {
	runners         []Runner
	shutdownTimeout time.Duration
	log             *slog.Logger
	health          *Health
}

// Option configures a Lane.
type Option func(*Lane)

// WithShutdownTimeout overrides the default 30s shutdown timeout.
func WithShutdownTimeout(d time.Duration) Option {
	return func(l *Lane) { l.shutdownTimeout = d }
}

// New constructs a Lane. log must not be nil.
func New(log *slog.Logger, opts ...Option) *Lane {
	l := &Lane{
		log:             log,
		shutdownTimeout: defaultShutdownTimeout,
		health:          &Health{},
	}
	for _, o := range opts {
		o(l)
	}
	return l
}

// AddRunner appends runners. Registration order determines startup order and
// reverse-order (LIFO) shutdown order.
func (l *Lane) AddRunner(runners ...Runner) {
	l.runners = append(l.runners, runners...)
}

// Health returns the Lane's lifecycle state and the liveness and readiness
// derived from it, e.g. for LivenessHandler and ReadinessHandler.
func (l *Lane) Health() *Health {
	return l.health
}

// Run starts all runners concurrently, blocks until a signal or runner failure,
// then shuts down runners in reverse registration order.
//
// Health (see State) moves Starting → Running once every runner implementing
// Readier reports ready; a Ready error is treated like a runner failure. If a
// runner fails or ctx is cancelled first, it goes straight to Stopping and
// never becomes ready. Shutdown moves it to Stopping (not ready, still live)
// and Run's return to Stopped (not live).
//
// Every runner receives Stop, even if an earlier Stop failed or the shutdown
// timeout expired; in the latter case runners get an already-expired ctx and
// must return promptly. Stop is called on every runner, including those whose
// Start already returned.
//
// A panic in Start, Ready, or Stop is recovered and treated as an error from
// that method, returned as a *PanicError: in Start or Ready it triggers
// shutdown, in Stop the remaining runners are still stopped. Panics in
// goroutines a runner spawns itself are not recovered; use Go for those.
//
// Shutdown is bounded by the shutdown timeout, shared by all Stop calls and
// the wait for Start to return. A Stop that outlives the deadline is recorded
// as context.DeadlineExceeded and Lane moves on. Runners reached after the
// deadline still receive Stop (with the expired ctx), but Lane does not wait
// for them and their result is only logged. If any Start has not returned by
// the deadline, Run returns without waiting for it and reports which runners
// were still running. Such runners are leaked, not killed.
//
// Cancellation vs. failure: cancelling ctx (or a SIGINT/SIGTERM) is an
// intentional shutdown. Any non-nil error from Start or Ready is a runner
// failure, including context.Canceled or context.DeadlineExceeded returned
// while Lane is still running. Once shutdown has begun (ctx cancelled, signal,
// or another runner failed), a Start that returns context.Canceled or
// context.DeadlineExceeded is treated as a clean exit; any other error is
// still a failure. A Start that returns nil before shutdown simply finishes;
// the other runners keep running.
//
// Returns nil on clean shutdown. Otherwise the first runner failure is the
// primary cause and is returned first, joined (errors.Join) with every Stop
// error, in stop order, and any shutdown timeout error. Later failures are
// logged but not returned. Use errors.Is / errors.As on the result.
func (l *Lane) Run(ctx context.Context) error {
	l.health.transition(StateStarting) // no-op on first run; resets after a previous Run
	defer l.health.transition(StateStopped)

	sigCh, stopSignals := setupSignals()
	defer stopSignals()

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	g, gctx := errgroup.WithContext(ctx)
	running := newRunningSet(l.runners)

	for i, r := range l.runners {
		g.Go(func() error {
			defer running.done(i)
			if err := callRunner(l.log, r.Name(), "start", func() error { return r.Start(gctx) }); err != nil {
				if gctx.Err() != nil && isContextErr(err) {
					return nil // the runner is reporting the shutdown, not a failure
				}
				l.log.Error("runner failed", "runner", r.Name(), "error", err)
				running.fail(err)
				return err
			}
			return nil
		})
	}

	readyCh := make(chan struct{})
	readyID := running.add("readiness check")
	g.Go(func() error {
		defer running.done(readyID)
		if err := l.waitReady(gctx); err != nil {
			if gctx.Err() != nil {
				return nil
			}
			l.log.Error("runner readiness failed", "error", err)
			running.fail(err)
			return err
		}
		close(readyCh)
		return nil
	})

	// The Running transition happens on this goroutine, so it can never race
	// with the Stopping transition below.
wait:
	for {
		select {
		case <-readyCh:
			readyCh = nil
			if gctx.Err() == nil {
				l.health.transition(StateRunning)
			}
		case <-sigCh:
			break wait
		case <-gctx.Done():
			l.log.Warn("runner context cancelled, initiating shutdown")
			break wait
		}
	}

	l.health.transition(StateStopping)
	cancel()

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), l.shutdownTimeout)
	defer shutdownCancel()

	var stopErrs []error
	for i := len(l.runners) - 1; i >= 0; i-- {
		r := l.runners[i]
		if err := l.stopRunner(shutdownCtx, r); err != nil {
			l.log.Error("runner stop error", "runner", r.Name(), "error", err)
			stopErrs = append(stopErrs, fmt.Errorf("stop %s: %w", r.Name(), err))
		}
	}

	waitCh := make(chan error, 1)
	go func() { waitCh <- g.Wait() }()

	var runErr error
	select {
	case runErr = <-waitCh:
	case <-shutdownCtx.Done():
		names := running.names()
		if len(names) == 0 {
			// Everything exited right at the deadline; g.Wait returns at once.
			runErr = <-waitCh
			break
		}
		l.log.Error("runners did not exit before shutdown timeout", "runners", names)
		runErr = errors.Join(running.firstErr(),
			fmt.Errorf("runners did not exit before shutdown timeout (%s): %w",
				strings.Join(names, ", "), shutdownCtx.Err()))
	}

	return errors.Join(append([]error{runErr}, stopErrs...)...)
}

// isContextErr reports whether err is a context cancellation or deadline error.
func isContextErr(err error) bool {
	return errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
}

// stopRunner calls r.Stop and returns once it does or ctx is done, whichever
// comes first. A Stop that ignores ctx keeps running in the background. If ctx
// has already expired, Stop is called without waiting and nil is returned.
func (l *Lane) stopRunner(ctx context.Context, r Runner) error {
	expired := ctx.Err() != nil
	done := make(chan error, 1)
	go func() {
		err := callRunner(l.log, r.Name(), "stop", func() error { return r.Stop(ctx) })
		if expired && err != nil {
			l.log.Error("runner stop error after shutdown timeout", "runner", r.Name(), "error", err)
		}
		done <- err
	}()
	if expired {
		return nil
	}
	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		select {
		case err := <-done: // Stop finished at the deadline; prefer its result
			return err
		default:
			return ctx.Err()
		}
	}
}

// runningSet tracks which runners' Start has not yet returned, and the first
// Start error, so Run can report them if shutdown times out.
type runningSet struct {
	mu      sync.Mutex
	pending map[int]string
	next    int
	first   error
}

// newRunningSet tracks one entry per runner, keyed by its index.
func newRunningSet(runners []Runner) *runningSet {
	s := &runningSet{pending: make(map[int]string, len(runners)), next: len(runners)}
	for i, r := range runners {
		s.pending[i] = r.Name()
	}
	return s
}

// add tracks an extra goroutine and returns its id.
func (s *runningSet) add(name string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	id := s.next
	s.next++
	s.pending[id] = name
	return id
}

func (s *runningSet) done(i int) {
	s.mu.Lock()
	delete(s.pending, i)
	s.mu.Unlock()
}

func (s *runningSet) fail(err error) {
	s.mu.Lock()
	if s.first == nil {
		s.first = err
	}
	s.mu.Unlock()
}

func (s *runningSet) firstErr() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.first
}

// names returns the pending runner names in registration order.
func (s *runningSet) names() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	idx := make([]int, 0, len(s.pending))
	for i := range s.pending {
		idx = append(idx, i)
	}
	slices.Sort(idx)
	out := make([]string, 0, len(idx))
	for _, i := range idx {
		out = append(out, s.pending[i])
	}
	return out
}

// waitReady blocks until every Readier runner is ready, one fails, or ctx is done.
func (l *Lane) waitReady(ctx context.Context) error {
	g, gctx := errgroup.WithContext(ctx)
	for _, r := range l.runners {
		rd, ok := r.(Readier)
		if !ok {
			continue
		}
		name := r.Name()
		g.Go(func() error {
			if err := callRunner(l.log, name, "ready", func() error { return rd.Ready(gctx) }); err != nil {
				return fmt.Errorf("ready %s: %w", name, err)
			}
			return nil
		})
	}
	return g.Wait()
}
