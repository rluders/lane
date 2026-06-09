package lane

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
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
