# ADR 0003: Criteria for built-in runners

- Status: accepted
- Issue: #50

## Context

Users will ask for more built-in runners. Each one adds maintenance, and too
many would turn Lane into a framework. This ADR sets the bar for inclusion and
applies it to the current candidates.

## Acceptance criteria

A runner belongs in `lane/v2/runners` only if **all** of these hold:

1. **Broadly reusable.** It solves a lifecycle problem many services share,
   not one domain's integration.
2. **Standard library only.** No third-party dependencies. Integrations with
   specific brokers, databases or SDKs belong in separate modules.
3. **Well-defined lifecycle.** Clear `Start`/`WaitReady`/`Stop` semantics,
   resource ownership and shutdown behavior that can be documented and
   tested.
4. **Uses the existing contract.** Implementable with `Runner`/`Readier` and
   the shared `runners.Option`, with no changes to the core.
5. **Minimal configuration.** Little to configure, without imposing
   application architecture.
6. **Platform-neutral.** Works on every platform Go supports.

## Candidates

| Candidate | Decision | Reason |
|---|---|---|
| HTTP server | exists (`HTTPRunner`) | |
| Background worker | exists (`WorkerRunner`) | |
| Scheduled job | exists (`SchedulerRunner`) | |
| **Function adapter** | **added: `FuncRunner`** | The most common custom runner is "run this function until cancelled". Trivial, stdlib-only, no design questions. Optional `WithStop`. |
| **TCP server** | **added: `TCPRunner`** | The non-HTTP counterpart of `HTTPRunner` (custom protocols, proxies). Stdlib-only. Clear lifecycle: bind fails startup, ready on bind, graceful drain then forced close at the deadline. |
| Message / queue consumers | rejected | Need broker SDKs (criterion 2), and semantics like ack, redelivery and prefetch are broker-specific. Use `WorkerRunner` or `FuncRunner` around your client, or a separate module. |
| Signal handling | rejected | Already part of the core (`Run` handles SIGINT/SIGTERM). |
| Periodic health checks | rejected | `SchedulerRunner` already covers it; health semantics belong to `ReadinessHandler` checks. |
| UDP / Unix-socket servers | deferred | Possible later under the same criteria if there's demand; not added speculatively. |
| gRPC server | rejected | Third-party dependency. The [server pattern](../runners.md#server-backed-component-with-readiness) shows a ~30-line runner. |

## Consequences

- Two stdlib-only runners are added, and the core API is unchanged.
- Future requests are judged against the criteria above. A request that fails
  them is answered with a pattern in `docs/runners.md` or a pointer to a
  separate module.
