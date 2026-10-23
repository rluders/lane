package lane

import "context"

// Runner is the core lifecycle abstraction. See docs/runners.md for
// implementation guidelines.
//
// Start MUST block until the runner stops or fails, and MUST return once ctx
// is cancelled (nil or ctx.Err() then count as a clean exit). Returning nil
// earlier means the runner finished; a non-nil error is a failure that shuts
// Lane down.
//
// Stop initiates a graceful shutdown and SHOULD return once it completes; it
// MUST respect the ctx deadline. Lane calls Stop exactly once per Run for every
// runner it started, even if Start already returned or failed, and possibly
// with an expired ctx.
type Runner interface {
	Name() string
	Start(ctx context.Context) error
	Stop(ctx context.Context) error
}

// Readier is optionally implemented by Runners that need time to become ready
// after Start is called (e.g. binding a listener, connecting to a broker).
// WaitReady MUST block until the runner is ready, it fails, or ctx is done.
// It may be called before Start begins. Lane reports ready only after every
// Readier's WaitReady returns nil; Runners that do not implement it count as
// ready once their Start has been launched.
type Readier interface {
	WaitReady(ctx context.Context) error
}
