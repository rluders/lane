# ADR 0001: Lifecycle observer instead of an event bus or built-in telemetry

- Status: accepted
- Issues: #45 (structured events), #47 (observability hooks),
  #48 (OpenTelemetry), #49 (metrics hooks)

## Context

Applications want to log, measure and trace Lane's lifecycle: how long startup
takes, which runner failed, how long each `Stop` took. Lane already logs, but
logs aren't a stable integration point, and Lane must not depend on any
logging, metrics or tracing provider.

## Options considered

1. **Event bus / channel of events.** Asynchronous delivery needs buffering
   and drop policies, can't guarantee ordering against the lifecycle, and adds
   goroutines and memory to every Lane. Too much machinery.
2. **One callback per hook** (`OnRunnerStart`, `OnRunnerStop`, …). Every new
   event adds API surface, and adapters need to implement N methods.
3. **Built-in OpenTelemetry / Prometheus support.** Couples Lane to specific
   providers and their release cadence. Rejected by the project's scope.
4. **A single observer function receiving structured events.** Chosen.

## Decision

`lane.WithObserver(func(lane.Event))`, which may be registered several times.

- `Event{Kind, Runner, Err, Time, Duration}`. Nine kinds cover lane and
  runner startup, readiness, shutdown and exit: `LaneStarting`,
  `RunnerStarting`, `RunnerReady`, `LaneReady`, `LaneStopping`,
  `RunnerStopping`, `RunnerStopped`, `RunnerExited`, `LaneStopped`. `Err` and
  `Duration` carry the failure and timing for kinds where they apply.
- **Synchronous and serialized.** Observers are never called concurrently and
  see events in lifecycle order. This is simple for implementers (no locking)
  and gives exact ordering. The cost is that a slow observer slows the
  lifecycle, so observers must be fast and hand off slow work themselves.
- **Ordering.** `LaneStarting` is first and `LaneStopped` last, with nothing
  delivered after it. `RunnerStarting` comes in registration order.
  `RunnerReady` events come before `LaneReady`. `LaneStopping` comes before
  any `RunnerStopping`, and stops follow LIFO order.
- **Failure isolation.** A panicking observer is recovered and logged and
  never affects the lifecycle. Observers have no return value, so they can't
  veto or delay transitions beyond their own execution time.
- **No new goroutines or buffers.** With no observers registered, emitting is
  a no-op.

## Consequences

- Logging, metrics (#49) and tracing (#48) are adapters written in the
  application or in separate modules on top of `Observer`. Examples are in
  `docs/observability.md`. Lane stays dependency-free.
- New event kinds can be added later without breaking observers, as long as
  observers ignore unknown kinds.
- Lane's own `slog` logging stays as is; observers are an addition, not a
  replacement.
