package runners

import "context"

// FuncRunner adapts a function to a Runner: Start calls fn and returns its
// result, so fn must block until ctx is cancelled (or return early to finish).
// Stop calls the function set with WithStop, if any.
//
// Use it for small components that don't warrant their own type.
type FuncRunner struct {
	name string
	fn   Func
	stop func(ctx context.Context) error
}

// NewFuncRunner creates a FuncRunner. Applicable options: WithStop.
func NewFuncRunner(name string, fn Func, opts ...Option) *FuncRunner {
	o := newOptions(opts)
	return &FuncRunner{name: name, fn: fn, stop: o.stop}
}

func (r *FuncRunner) Name() string { return r.name }

func (r *FuncRunner) Start(ctx context.Context) error { return r.fn(ctx) }

func (r *FuncRunner) Stop(ctx context.Context) error {
	if r.stop == nil {
		return nil
	}
	return r.stop(ctx)
}
