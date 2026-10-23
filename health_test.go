package lane

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestHealthState_SetReady(t *testing.T) {
	h := &HealthState{}

	if h.IsReady() {
		t.Fatal("new HealthState should not be ready")
	}

	h.SetReady(true)
	if !h.IsReady() {
		t.Fatal("expected ready after SetReady(true)")
	}

	h.SetReady(false)
	if h.IsReady() {
		t.Fatal("expected not ready after SetReady(false)")
	}
}

func TestHealthState_Concurrent(t *testing.T) {
	h := &HealthState{}
	var wg sync.WaitGroup

	for range 100 {
		wg.Add(2)
		go func() { defer wg.Done(); h.SetReady(true) }()
		go func() { defer wg.Done(); _ = h.IsReady() }()
	}
	wg.Wait()
}

func TestLivenessHandler(t *testing.T) {
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	LivenessHandler()(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}

	var body map[string]string
	if err := json.NewDecoder(rec.Body).Decode(&body); err != nil {
		t.Fatal(err)
	}
	if body["status"] != "ok" {
		t.Fatalf("expected status=ok, got %q", body["status"])
	}
}

func TestReadinessHandler_NotReady(t *testing.T) {
	h := &HealthState{}
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/ready", nil)
	ReadinessHandler(h)(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503, got %d", rec.Code)
	}
}

func TestReadinessHandler_Ready(t *testing.T) {
	h := &HealthState{}
	h.SetReady(true)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/ready", nil)
	ReadinessHandler(h)(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
}

func TestReadinessHandler_CheckFails(t *testing.T) {
	h := &HealthState{}
	h.SetReady(true)

	failCheck := func(ctx context.Context) error {
		return errors.New("db unreachable")
	}

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/ready", nil)
	ReadinessHandler(h, failCheck)(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503, got %d", rec.Code)
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
	if gotPath != "/health" {
		t.Fatalf("path = %q, want /health", gotPath)
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
