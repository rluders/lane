package lane

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func healthIn(s State) *Health { return &Health{state: s} }

func serve(h http.HandlerFunc) (int, map[string]string) {
	rec := httptest.NewRecorder()
	h(rec, httptest.NewRequest(http.MethodGet, "/", nil))
	var body map[string]string
	_ = json.NewDecoder(rec.Body).Decode(&body)
	return rec.Code, body
}

func TestLivenessHandler(t *testing.T) {
	for _, tt := range []struct {
		state State
		code  int
	}{
		{StateStarting, http.StatusOK},
		{StateRunning, http.StatusOK},
		{StateStopping, http.StatusOK},
		{StateStopped, http.StatusServiceUnavailable},
	} {
		code, body := serve(LivenessHandler(healthIn(tt.state)))
		if code != tt.code || body["state"] != tt.state.String() {
			t.Errorf("%v: got %d %v, want %d", tt.state, code, body, tt.code)
		}
	}
}

func TestReadinessHandler(t *testing.T) {
	for _, tt := range []struct {
		state State
		code  int
	}{
		{StateStarting, http.StatusServiceUnavailable},
		{StateRunning, http.StatusOK},
		{StateStopping, http.StatusServiceUnavailable},
		{StateStopped, http.StatusServiceUnavailable},
	} {
		code, body := serve(ReadinessHandler(healthIn(tt.state)))
		if code != tt.code || body["state"] != tt.state.String() {
			t.Errorf("%v: got %d %v, want %d", tt.state, code, body, tt.code)
		}
	}
}

func TestReadinessHandler_CheckFails(t *testing.T) {
	failCheck := func(ctx context.Context) error { return errors.New("db unreachable") }
	code, _ := serve(ReadinessHandler(healthIn(StateRunning), failCheck))
	if code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503, got %d", code)
	}
}

func newHealthServer(t *testing.T, handler http.HandlerFunc) (hostPort, port string) {
	t.Helper()
	srv := httptest.NewServer(handler)
	t.Cleanup(srv.Close)
	hostPort = srv.Listener.Addr().String()
	_, port, err := net.SplitHostPort(hostPort)
	if err != nil {
		t.Fatal(err)
	}
	return hostPort, port
}

func TestCheckHealth_DefaultPath(t *testing.T) {
	var gotPath string
	addr, _ := newHealthServer(t, func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
	})
	if err := CheckHealth(context.Background(), addr); err != nil {
		t.Fatalf("CheckHealth: %v", err)
	}
	if gotPath != "/live" {
		t.Fatalf("path = %q, want /live", gotPath)
	}
}

func TestCheckHealth_PortOnlyAddr(t *testing.T) {
	_, port := newHealthServer(t, func(w http.ResponseWriter, r *http.Request) {})
	if err := CheckHealth(context.Background(), ":"+port); err != nil {
		t.Fatalf("CheckHealth with :port: %v", err)
	}
}

func TestCheckHealth_CustomPath(t *testing.T) {
	addr, _ := newHealthServer(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/live" {
			w.WriteHeader(http.StatusNotFound)
		}
	})
	if err := CheckHealth(context.Background(), addr, WithHealthPath("/live")); err != nil {
		t.Fatalf("CheckHealth: %v", err)
	}
	if err := CheckHealth(context.Background(), addr, WithHealthPath("live")); err != nil {
		t.Fatalf("CheckHealth without leading slash: %v", err)
	}
}

func TestCheckHealth_UnsuccessfulStatus(t *testing.T) {
	addr, _ := newHealthServer(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	})
	err := CheckHealth(context.Background(), addr)
	if err == nil || !strings.Contains(err.Error(), "503") {
		t.Fatalf("expected 503 error, got %v", err)
	}
	if err := CheckHealth(context.Background(), addr, WithExpectedStatus(http.StatusServiceUnavailable)); err != nil {
		t.Fatalf("with expected 503: %v", err)
	}
}

func TestCheckHealth_Timeout(t *testing.T) {
	release := make(chan struct{})
	addr, _ := newHealthServer(t, func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-release:
		case <-r.Context().Done():
		}
	})
	defer close(release)

	start := time.Now()
	err := CheckHealth(context.Background(), addr, WithHealthTimeout(50*time.Millisecond))
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected deadline exceeded, got %v", err)
	}
	if d := time.Since(start); d > time.Second {
		t.Fatalf("timeout took %v", d)
	}
}

func TestCheckHealth_Unreachable(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	ln.Close() // nothing listening now
	if err := CheckHealth(context.Background(), addr); err == nil {
		t.Fatal("expected error for unreachable endpoint")
	}
}
