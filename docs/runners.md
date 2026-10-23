# Writing and using Runners

How to implement a correct `lane.Runner`, and how the built-in runners
behave. The lifecycle guarantees Lane provides are in
[lifecycle.md](lifecycle.md).

- [The contract](#the-contract)
- [Patterns](#patterns)
- [Common mistakes](#common-mistakes)
- [Testing a Runner](#testing-a-runner)
- [Built-in: WorkerRunner](#workerrunner)
- [Built-in: SchedulerRunner](#schedulerrunner)
- [Built-in: HTTPRunner](#httprunner)
- [Built-in: TCPRunner](#tcprunner)
- [Built-in: FuncRunner](#funcrunner)

## The contract

```go
type Runner interface {
    Name() string
    Start(ctx context.Context) error
    Stop(ctx context.Context) error
}

// Optional.
type Readier interface {
    WaitReady(ctx context.Context) error
}
```

Runners don't need to import `lane`; implementing the methods is enough.

### `Name()`

A short, stable identifier used in logs and errors (`stop <name>: …`,
`*PanicError.Runner`). It should be unique within a Lane.

### `Start(ctx)`

- **Blocks for the component's whole lifetime** and returns when it stops.
  Returning early means "I'm done": Lane treats `nil` as a clean finish, not
  a reason to shut down.
- **Must return once `ctx` is cancelled.** Lane cancels `ctx` when shutdown
  begins. Returning `nil` or `ctx.Err()` at that point is a clean exit.
  A `Start` that ignores `ctx` is abandoned after the shutdown timeout.
- **A non-nil error is a failure** while Lane is running, and triggers
  shutdown of every runner. Wrap errors with context (`fmt.Errorf("consumer: %w", err)`).
- May be called with an already-cancelled `ctx`. Return promptly.
- Is called once per `Lane.Run`.

### `WaitReady(ctx)` (optional)

- Blocks until the runner can do its job (listener bound, connection
  established, cache warmed), then returns nil.
- **Must return when `ctx` is done.** Return an error if startup failed.
- May be called **before `Start` begins**, so don't assume `Start` has run.
  Initialize whatever it waits on (e.g. a channel) in the constructor.
- Implement it only if readiness lags behind launching. Otherwise the runner
  counts as ready once `Start` is launched.

### `Stop(ctx)`

- Initiates graceful shutdown and **should return when shutdown is complete**
  or when `ctx` is done, whichever comes first. `ctx` carries the shutdown
  deadline.
- Called **exactly once per `Run`** for every runner whose `Start` was called,
  even if `Start` already returned, failed, or never got far. It may also run concurrently with a `Start` that hasn't
  noticed cancellation yet, so it must be safe in every one of those cases.
- May be called with an already-expired `ctx` (shutdown timeout passed).
  Return immediately.
- Return an error only for a real shutdown problem (failed flush, unclean
  close). It is reported in `Run`'s error, but the other runners still stop.
- If cancelling `Start`'s `ctx` is all the shutdown you need, `Stop` can
  simply `return nil`.

### Goroutines and resources

- **A runner owns every goroutine it starts** and must make them exit by the
  time `Start` returns (or `Stop` completes). Leaked goroutines outlive the
  Lane.
- Lane recovers panics only in `Start`, `WaitReady` and `Stop`. Start
  critical background goroutines with `lane.Go(ctx, log, name, cancel, fn)`
  so a panic is logged and turned into a shutdown instead of a crash.
- Release what you acquire. Acquire in `Start` (or the constructor), release
  in `Stop` or before `Start` returns, and make release safe if acquisition
  never happened.

## Patterns

### Long-running loop

```go
type Consumer struct{ q Queue }

func (c *Consumer) Name() string { return "consumer" }

func (c *Consumer) Start(ctx context.Context) error {
    for {
        msg, err := c.q.Receive(ctx)
        if err != nil {
            return err // ctx.Err() after shutdown is a clean exit
        }
        c.handle(ctx, msg)
    }
}

func (c *Consumer) Stop(context.Context) error { return nil } // ctx cancellation stops Start
```

For a loop that should keep going after errors, use the built-in
[`WorkerRunner`](#workerrunner) rather than writing your own backoff.

### Server-backed component with readiness

```go
type GRPC struct {
    srv   *grpc.Server
    addr  string
    ready chan struct{}
}

func NewGRPC(srv *grpc.Server, addr string) *GRPC {
    return &GRPC{srv: srv, addr: addr, ready: make(chan struct{})}
}

func (g *GRPC) Name() string { return "grpc" }

func (g *GRPC) Start(ctx context.Context) error {
    ln, err := net.Listen("tcp", g.addr)
    if err != nil {
        return err // fails startup; Lane never becomes ready
    }
    close(g.ready)
    return g.srv.Serve(ln) // returns once Stop calls GracefulStop
}

func (g *GRPC) WaitReady(ctx context.Context) error {
    select {
    case <-g.ready:
        return nil
    case <-ctx.Done():
        return ctx.Err()
    }
}

func (g *GRPC) Stop(ctx context.Context) error {
    done := make(chan struct{})
    go func() { g.srv.GracefulStop(); close(done) }()
    select {
    case <-done:
        return nil
    case <-ctx.Done():
        g.srv.Stop() // deadline hit: force close
        return ctx.Err()
    }
}
```

### Component that owns background goroutines

```go
func (c *Cache) Start(ctx context.Context) error {
    ctx, cancel := context.WithCancel(ctx)
    defer cancel()

    var wg sync.WaitGroup
    for name, loop := range map[string]func(context.Context){
        "cache-refresh": c.refreshLoop,
        "cache-evict":   c.evictLoop,
    } {
        wg.Add(1)
        // A panic is logged and calls cancel, so Start returns.
        lane.Go(ctx, c.log, name, cancel, func(ctx context.Context) {
            defer wg.Done()
            loop(ctx)
        })
    }

    <-ctx.Done()
    wg.Wait() // don't return while our goroutines are still running
    return nil
}
```

### Initialize, then stay active

Do the initialization in `Start`, signal readiness, then block:

```go
func (d *DB) Start(ctx context.Context) error {
    if err := d.pool.Ping(ctx); err != nil {
        return fmt.Errorf("db: %w", err)
    }
    close(d.ready)
    <-ctx.Done()
    return nil
}

func (d *DB) Stop(ctx context.Context) error { return d.pool.Close() }
```

Put the DB in an earlier stage than its dependents. They then start only
after it is ready, and are stopped before it:

```go
l.AddStage(db)
l.AddStage(api, consumer)
```

A pure one-shot task (e.g. a migration) can return nil from `Start` once it's
done. Lane keeps running the others. If later runners need it to finish
first, give it a `WaitReady` that returns once the migration completes and
put it in an earlier stage, or run it before `Lane.Run`.

## Common mistakes

- **Returning from `Start` immediately** after launching a goroutine. Lane
  treats the runner as finished, and failures in that goroutine go unseen.
  Block instead.
- **Ignoring `ctx`** in `Start`, `WaitReady` or `Stop`. Shutdown then waits
  for the full timeout and the runner is leaked.
- **Returning a custom error on shutdown** instead of `nil`/`ctx.Err()`. It is
  reported as a failure.
- **`WaitReady` assuming `Start` ran.** Create the ready signal in the
  constructor.
- **`Stop` that panics or blocks** when `Start` never ran (nil fields, closing
  an already-closed channel).
- **Unprotected background goroutines.** A panic there crashes the process;
  use `lane.Go`.
- **Relying on registration order for startup.** Runners within a stage start
  concurrently. Use `AddStage` plus `WaitReady` for "start B after A is up".

## Testing a Runner

Test it without Lane, the way Lane drives it:

```go
func TestConsumer(t *testing.T) {
    r := NewConsumer(fakeQueue)
    ctx, cancel := context.WithCancel(context.Background())
    done := make(chan error, 1)
    go func() { done <- r.Start(ctx) }()

    // if r implements Readier:
    rctx, rcancel := context.WithTimeout(context.Background(), time.Second)
    defer rcancel()
    if err := r.WaitReady(rctx); err != nil { t.Fatal(err) }

    // ... exercise it ...

    cancel()
    sctx, scancel := context.WithTimeout(context.Background(), time.Second)
    defer scancel()
    if err := r.Stop(sctx); err != nil { t.Fatal(err) }
    if err := <-done; err != nil && !errors.Is(err, context.Canceled) { t.Fatal(err) }
}
```

Also test `Stop` without `Start`, `Start` with an already-cancelled context,
and `WaitReady` with a context that expires.

## Built-in runners

All live in `github.com/rluders/lane/v2/runners` and share one `Option` type.
`WithLogger` (default `slog.Default()`) and `WithErrorHandler` apply to all of
them; options that don't apply to a runner are ignored.

### WorkerRunner

`runners.NewWorkerRunner(name, fn, opts...)` calls `fn(ctx)` in a loop until
`ctx` is cancelled.

- `fn` returns nil → the next iteration runs immediately.
- `fn` returns an error → the error handler is called (default: log at
  error level), then the worker **waits before retrying**: 100ms after the
  first consecutive failure, doubling up to 30s. A success resets the delay.
  This prevents tight loops when a dependency is down.
  - `WithBackoff(initial, limit)` tunes the delays. Values ≤ 0 keep defaults,
    and `limit` is raised to `initial` if smaller.
  - `WithBackoffJitter()` randomizes each delay within [d/2, d] so many
    workers don't retry in lockstep. Without it, delays are deterministic.
- Errors **never stop** the worker and never affect Lane's state. To make an
  error fatal, return it from a custom runner instead.
- The backoff wait is interrupted immediately by cancellation. Errors
  returned after `ctx` is cancelled are not reported.
- A panic in `fn` propagates out of `Start`; Lane recovers it and shuts down.
- `Stop` is a no-op; cancellation of `Start`'s context stops the loop.

### SchedulerRunner

`runners.NewSchedulerRunner(name, interval, fn, opts...)` calls `fn(ctx)` on a
fixed schedule.

- **Fixed rate**: runs happen at ticks every `interval`, measured from
  `Start`, not from the end of the previous run.
- **First run** one `interval` after `Start`, or immediately with
  `WithImmediateRun()`. An immediate run doesn't shift the tick schedule.
- **No overlap**: runs never run concurrently. Ticks that pass while a run is
  in progress are **dropped**, not queued, so after a slow run the next one
  waits for the next tick.
- **Cancellation**: no new run starts after `ctx` is cancelled. A running
  `fn` receives the cancelled `ctx`, and `Start` returns once it does.
- **Errors** go to the error handler (default: log) and scheduling continues.
  No special handling for consecutive failures, no retry or backoff. Put those
  inside `fn` if needed. Errors after cancellation are not reported.
- `interval <= 0` makes `Start` return an error.
- A panic in `fn` propagates out of `Start`; Lane recovers it and shuts down.

### HTTPRunner

`runners.NewHTTPRunner(name, server, opts...)` serves an `*http.Server`.

- Binds the listener in `Start`; a bind error fails startup.
- Implements `Readier`: ready once the listener is bound.
- `Stop` calls `server.Shutdown(ctx)`, which drains in-flight requests until
  the shutdown deadline.
- `WithTLS(certFile, keyFile)` serves TLS. Everything else (timeouts,
  handler, `TLSConfig`) is configured on the `*http.Server` you pass in.

### TCPRunner

`runners.NewTCPRunner(name, addr, handler, opts...)` accepts TCP connections
and serves each one with `handler(ctx, conn)` in its own goroutine.

- Binds in `Start`; a bind (or TLS key loading) error fails startup.
- Implements `Readier`: ready once bound. `Addr()` returns the bound address,
  which is useful with port `0`.
- When shutdown begins, the listener closes and the handlers' `ctx` is
  cancelled. Handlers should finish their current exchange and return. `Stop`
  waits for them until its deadline, then force-closes the remaining
  connections and returns `context.DeadlineExceeded`.
- The runner closes each `conn` after its handler returns. `Start` returns
  only after every handler has returned.
- Temporary accept errors are retried with a short backoff (5ms–1s). Other
  accept errors make `Start` return the error.
- `WithTLS(certFile, keyFile)` serves TLS.

```go
echo := runners.NewTCPRunner("echo", ":9000", func(ctx context.Context, c net.Conn) {
    io.Copy(c, c)
})
```

### FuncRunner

`runners.NewFuncRunner(name, fn, opts...)` turns a function into a runner:
`Start` calls `fn(ctx)` and returns its result, so `fn` must block until `ctx`
is cancelled (returning earlier means "finished"). `WithStop(fn)` sets what
`Stop` does. Without it, `Stop` is a no-op.

```go
l.AddRunner(runners.NewFuncRunner("consumer", func(ctx context.Context) error {
    return client.Consume(ctx, handle) // blocks until ctx is cancelled
}, runners.WithStop(func(context.Context) error { return client.Close() })))
```

New built-in runners must meet the criteria in
[ADR 0003](adr/0003-built-in-runners.md).

