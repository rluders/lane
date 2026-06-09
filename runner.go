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
