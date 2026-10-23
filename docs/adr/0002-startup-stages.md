# ADR 0002: Ordered startup stages instead of a dependency graph

- Status: accepted
- Issues: #42 (explicit dependencies), #43 (dependency-aware startup and
  shutdown), #44 (circular dependency detection)

## Context

Real services have startup dependencies: a consumer needs the broker
connection, the API needs the database. Before this change Lane started every
runner concurrently. Shutdown was already reverse registration order, so
dependents stopped first, but there was no way to say "start B only after A is
up". The workarounds were initializing before `Run` (which loses Lane's
cancellation and readiness handling) or making each runner poll its
dependency.

## Options considered

1. **No feature, documentation only.** Leaves a common need unsolved.
2. **Dependency graph** (`AddRunner(api, DependsOn("db"))`). Gives maximum
   concurrency, but adds name resolution, unknown-name and cycle errors, a
   topological scheduler, and graph-shaped shutdown. That's a large share of
   Lane's code for an edge in flexibility most services don't need.
3. **Ordered stages.** Chosen.

## Decision

`(*Lane).AddStage(runners...)`, additive and backward compatible.

- Stages form a sequence. A stage starts when every runner in earlier stages
  is ready (`WaitReady` returned nil, or the runner was launched if it isn't a
  `Readier`). Runners within a stage start concurrently.
- `AddRunner` appends to the current stage, so code that never calls
  `AddStage` has a single stage and behaves exactly as before.
- **Shutdown is unchanged**: the reverse of registration order. That already
  means later stages first and LIFO within a stage.
- **Failure and cancellation**: if startup fails or is cancelled while an
  earlier stage is starting, later stages are never started, never stopped,
  and emit no events. Runtime failures after all stages are up behave as
  before.
- **Readiness**: Lane is `Running` only after the last stage is ready.
- **Cycles and invalid references (#44)** can't exist: there are no names or
  edges, only an ordered list. Nothing needs to be detected.

## Consequences

- Independent runners stay concurrent within a stage. Serialization happens
  only at the stage boundaries the user chooses.
- Some graphs can't be expressed with maximum concurrency. For example, if C
  needs only A, and B is slow, C still waits for B when A and B share a stage.
  Accepted: putting C in its own stage or adjusting stages covers real
  services, and an explicit graph can still be added later if it proves
  necessary.
- The Stop guarantee is now "every *started* runner is stopped exactly once".
