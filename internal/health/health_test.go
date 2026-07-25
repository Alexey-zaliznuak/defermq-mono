package health

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/defermq/defermq/internal/buildinfo"
)

func TestRegistryReportsSortedChecksAndFailures(t *testing.T) {
	registry := NewRegistry(100 * time.Millisecond)
	registry.MustRegister("postgres", func(context.Context) error { return nil })
	registry.MustRegister("nats", func(context.Context) error { return errors.New("offline") })

	ready, results := registry.Check(context.Background())
	if ready {
		t.Fatal("registry should not be ready")
	}
	if len(results) != 2 || results[0].Name != "nats" || results[1].Name != "postgres" {
		t.Fatalf("checks are not sorted: %+v", results)
	}
	if results[0].Ready || !results[1].Ready {
		t.Fatalf("unexpected check states: %+v", results)
	}
}

func TestRegistryBoundsCheckDuration(t *testing.T) {
	registry := NewRegistry(10 * time.Millisecond)
	registry.MustRegister("slow", func(ctx context.Context) error {
		<-ctx.Done()
		return ctx.Err()
	})
	started := time.Now()
	ready, _ := registry.Check(context.Background())
	if ready || time.Since(started) > 200*time.Millisecond {
		t.Fatal("check timeout was not enforced")
	}
}

func TestHandlersExposeSafeStatus(t *testing.T) {
	registry := NewRegistry(time.Second)
	registry.MustRegister("postgres", func(context.Context) error {
		return errors.New("postgres://user:secret@db/defermq")
	})
	liveness := &Liveness{}
	handler := NewHandler("defermq-gateway", buildinfo.Info{Version: "v1.2.3", Commit: "abc"}, liveness, registry)

	readyRecorder := httptest.NewRecorder()
	handler.Ready(readyRecorder, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if readyRecorder.Code != http.StatusServiceUnavailable {
		t.Fatalf("ready status = %d", readyRecorder.Code)
	}
	if strings.Contains(readyRecorder.Body.String(), "secret") || !strings.Contains(readyRecorder.Body.String(), "check failed") {
		t.Fatalf("readiness leaked an error or omitted safe message: %s", readyRecorder.Body.String())
	}

	liveRecorder := httptest.NewRecorder()
	handler.Live(liveRecorder, httptest.NewRequest(http.MethodGet, "/livez", nil))
	if liveRecorder.Code != http.StatusOK || !strings.Contains(liveRecorder.Body.String(), "v1.2.3") {
		t.Fatalf("unexpected liveness response: %d %s", liveRecorder.Code, liveRecorder.Body.String())
	}

	liveness.MarkShuttingDown()
	shutdownRecorder := httptest.NewRecorder()
	handler.Live(shutdownRecorder, httptest.NewRequest(http.MethodGet, "/livez", nil))
	if shutdownRecorder.Code != http.StatusServiceUnavailable {
		t.Fatalf("shutdown liveness status = %d", shutdownRecorder.Code)
	}
}

func TestStateTransitions(t *testing.T) {
	state := &State{}
	if state.Check(context.Background()) == nil {
		t.Fatal("uninitialized state should fail")
	}
	state.MarkReady()
	if err := state.Check(context.Background()); err != nil {
		t.Fatalf("ready state returned %v", err)
	}
	state.MarkFailed(errors.New("loop stopped"))
	if err := state.Check(context.Background()); err == nil {
		t.Fatal("failed state should return an error")
	}
}
