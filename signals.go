package lane

import (
	"os"
	"os/signal"
	"syscall"
)

func setupSignals() (<-chan os.Signal, func()) {
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, os.Interrupt, syscall.SIGTERM)
	return ch, func() { signal.Stop(ch) }
}
