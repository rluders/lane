# ADR 0004: Restarts are an opt-in runner wrapper, not core behavior

- Status: accepted
- Issue: #46

## Context

Some components fail transiently (a broker blip, a dropped connection), and
shutting down the whole service for that is heavier than needed. Automatic
restarts raise hard questions, though: which errors are recoverable, how
resources are cleaned up between attempts, what readiness means while
restarting, and how restarts interact with shutdown. Lane's core promises "no
hidden recovery".

## Options considered

1. **Restart policies in the core** (per-runner policy in `AddRunner`). Every
   runner would become restartable in principle. That means readiness
   regressions (Running → not ready → Running), new states, and interaction
   with stages and shutdown inside the lifecycle engine. Too much complexity
   in core.
2. **Nothing; leave it to applications.** Leaves everyone to reinvent the
   same backoff loop.
3. **An opt-in wrapper in `runners`.** Chosen.

## Decision

`runners.NewSupervisor(r, opts...)` returns a runner that wraps `r`:

- **Restart trigger:** `Start` returns an error or panics while the context
  is live. A nil return (finished) or cancellation never restarts.
- **Backoff:** reuses `WithBackoff`/`WithBackoffJitter` (default 100ms
  doubling to 30s). An attempt that ran longer than the backoff cap counts as
  healthy and resets both the backoff and the restart count.
- **Limits:** `WithMaxRestarts(n)` (default unlimited, like a crash-loop
  backoff). When exhausted, the last error is returned and Lane treats it as
  a normal runner failure, so the lifecycle contract is unchanged.
- **Failure classification:** none built in. A runner that knows an error is
  fatal can return it from a non-supervised runner, or use
  `WithMaxRestarts`. Keeping the wrapper simple beats a classification API.
- **Cleanup:** the wrapped runner's `Stop` is called only once, at Lane
  shutdown. Between attempts, `Start` must release its own resources before
  returning. That's the requirement for being restartable.
- **Readiness:** `WaitReady` is forwarded to the wrapped runner. Lane's
  readiness does not regress during restarts (Lane has no "unready again"
  state). Use `ReadinessHandler` checks if traffic must stop while a
  dependency is down.
- **Observability:** each failed attempt goes to the error handler (default:
  log). Exhausting the limit surfaces as the usual `RunnerExited` /
  `LaneStopping` events.
- **Eligibility:** only runners whose `Start` can run more than once.
  `WorkerRunner`, `SchedulerRunner` and `FuncRunner` qualify. `HTTPRunner`
  and `TCPRunner` do not, because their servers are single-use.

## Consequences

- The core stays free of supervision. Restarts are visible in code at the
  registration site (`NewSupervisor(...)`).
- `runners` gains `NewSupervisor`, `Restartable` and `WithMaxRestarts`, and
  still doesn't import the core package.
