package runners

import (
	"io"
	"log/slog"
	"net/http"
	"testing"
)

func TestOptions_Defaults(t *testing.T) {
	o := newOptions(nil)
	if o.log != slog.Default() {
		t.Fatal("default logger should be slog.Default()")
	}
	if o.baseBackoff != defaultWorkerBaseBackoff || o.maxBackoff != defaultWorkerMaxBackoff {
		t.Fatalf("default backoff: %v/%v", o.baseBackoff, o.maxBackoff)
	}
	if o.onError != nil || o.jitter || o.immediate || o.certFile != "" {
		t.Fatalf("unexpected non-zero defaults: %+v", o)
	}
}

func TestWithLogger_NilIgnored(t *testing.T) {
	custom := slog.New(slog.NewTextHandler(io.Discard, nil))
	if o := newOptions([]Option{WithLogger(custom)}); o.log != custom {
		t.Fatal("WithLogger not applied")
	}
	if o := newOptions([]Option{WithLogger(nil)}); o.log != slog.Default() {
		t.Fatal("WithLogger(nil) should keep the default")
	}
}

func TestSharedOptionsAcrossRunners(t *testing.T) {
	var got []string
	handler := WithErrorHandler(func(err error) { got = append(got, err.Error()) })
	w := NewWorkerRunner("w", nil, handler)
	s := NewSchedulerRunner("s", 1, nil, handler)
	w.onError(errWork)
	s.onError(errWork)
	if len(got) != 2 {
		t.Fatalf("WithErrorHandler should apply to both runners, got %v", got)
	}

	// Options for other runners are accepted and ignored.
	h := NewHTTPRunner("h", &http.Server{}, WithBackoff(1, 2), WithImmediateRun(), WithTLS("c.pem", "k.pem"))
	if h.certFile != "c.pem" || h.keyFile != "k.pem" {
		t.Fatalf("WithTLS not applied: %q %q", h.certFile, h.keyFile)
	}
}
