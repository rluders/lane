package lane

import "context"

// Runner is the core lifecycle abstraction.
// Start MUST block until the runner stops or fails.
// Stop initiates a graceful shutdown; it MUST respect ctx deadline.
type Runner interface {
	Name() string
	Start(ctx context.Context) error
	Stop(ctx context.Context) error
}

// Readier is optionally implemented by Runners that need time to become ready
// after Start is called (e.g. binding a listener, connecting to a broker).
// Ready MUST block until the runner is ready, it fails, or ctx is done.
// It may be called before Start begins. Lane reports ready only after every
// Readier returns nil; Runners that do not implement it count as ready once
// their Start has been launched.
type Readier interface {
	Ready(ctx context.Context) error
}
