# Observability

Lane has no dependency on any logging, metrics or tracing provider. It
delivers lifecycle events to observers, and you adapt them to whatever you use
(see [ADR 0001](adr/0001-lifecycle-observer.md)).

```go
l := lane.New(lane.WithObserver(func(e lane.Event) {
    // called synchronously, never concurrently, in lifecycle order
}))
```

## Events

| Kind | When | `Runner` | `Err` | `Duration` |
|---|---|---|---|---|
| `EventLaneStarting` | `Run` called | | | |
| `EventRunnerStarting` | before each `Start` (registration order) | ✅ | | |
| `EventRunnerReady` | `WaitReady` returned nil, or launched if not a `Readier` | ✅ | | since `LaneStarting` |
| `EventLaneReady` | every runner ready; state `Running` | | | since `LaneStarting` |
| `EventLaneStopping` | shutdown began | | primary failure, or nil for signal/ctx | |
| `EventRunnerStopping` | before each `Stop` (LIFO) | ✅ | | |
| `EventRunnerStopped` | `Stop` returned | ✅ | stop error; `DeadlineExceeded` if reached after the deadline | `Stop` call |
| `EventRunnerExited` | `Start` returned | ✅ | non-nil only for a failure | since `RunnerStarting` |
| `EventLaneStopped` | `Run` returning (always last) | | `Run`'s result | whole `Run` |

Rules:

- Observers are called **synchronously, one at a time**, in registration
  order. Keep them fast; send slow work to a channel or goroutine.
- A panic in an observer is recovered and logged and never affects the
  lifecycle.
- Nothing is delivered after `EventLaneStopped`. A runner abandoned at the
  shutdown timeout gets no `EventRunnerExited`.
- Ignore kinds you don't know; new kinds may be added.

## Structured logging (slog)

```go
func slogObserver(log *slog.Logger) lane.Observer {
    return func(e lane.Event) {
        level := slog.LevelInfo
        if e.Err != nil {
            level = slog.LevelError
        }
        attrs := []any{"event", e.Kind.String()}
        if e.Runner != "" {
            attrs = append(attrs, "runner", e.Runner)
        }
        if e.Duration > 0 {
            attrs = append(attrs, "duration", e.Duration)
        }
        if e.Err != nil {
            attrs = append(attrs, "error", e.Err)
        }
        log.Log(context.Background(), level, "lifecycle", attrs...)
    }
}
```

## Metrics (Prometheus)

```go
var (
    ready = promauto.NewGauge(prometheus.GaugeOpts{
        Name: "lane_ready", Help: "1 while the lane is Running."})
    startup = promauto.NewGauge(prometheus.GaugeOpts{
        Name: "lane_startup_seconds", Help: "Time from Run to ready."})
    stopDur = promauto.NewHistogramVec(prometheus.HistogramOpts{
        Name: "lane_runner_stop_seconds", Help: "Stop duration per runner."}, []string{"runner"})
    failures = promauto.NewCounterVec(prometheus.CounterOpts{
        Name: "lane_runner_failures_total", Help: "Runner failures."}, []string{"runner"})
)

func metricsObserver(e lane.Event) {
    switch e.Kind {
    case lane.EventLaneReady:
        ready.Set(1)
        startup.Set(e.Duration.Seconds())
    case lane.EventLaneStopping:
        ready.Set(0)
    case lane.EventRunnerStopped:
        stopDur.WithLabelValues(e.Runner).Observe(e.Duration.Seconds())
    case lane.EventRunnerExited:
        if e.Err != nil {
            failures.WithLabelValues(e.Runner).Inc()
        }
    }
}
```

## Tracing (OpenTelemetry)

A span for the whole run, plus one span per runner stop:

```go
func tracingObserver(tracer trace.Tracer) lane.Observer {
    var runSpan trace.Span
    var ctx context.Context
    stops := map[string]trace.Span{} // safe: observers are never called concurrently
    return func(e lane.Event) {
        switch e.Kind {
        case lane.EventLaneStarting:
            ctx, runSpan = tracer.Start(context.Background(), "lane.run",
                trace.WithTimestamp(e.Time))
        case lane.EventLaneReady:
            runSpan.AddEvent("ready")
        case lane.EventRunnerStopping:
            _, stops[e.Runner] = tracer.Start(ctx, "lane.stop "+e.Runner,
                trace.WithTimestamp(e.Time))
        case lane.EventRunnerStopped:
            if s := stops[e.Runner]; s != nil {
                if e.Err != nil {
                    s.RecordError(e.Err)
                }
                s.End(trace.WithTimestamp(e.Time))
                delete(stops, e.Runner)
            }
        case lane.EventLaneStopped:
            if e.Err != nil {
                runSpan.RecordError(e.Err)
                runSpan.SetStatus(codes.Error, e.Err.Error())
            }
            runSpan.End(trace.WithTimestamp(e.Time))
        }
    }
}
```
