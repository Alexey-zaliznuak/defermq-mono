package loadtest

import (
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestGatewayClientLifecycleRequests(t *testing.T) {
	const id = "0198f4f4-1b9e-7d3e-9c65-96c7af740db1"
	var calls []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		calls = append(calls, request.Method+" "+request.URL.Path)
		switch {
		case request.Method == http.MethodPost && request.URL.Path == "/v1/messages":
			if request.Header.Get("Idempotency-Key") != "load-1" {
				t.Errorf("idempotency key = %q", request.Header.Get("Idempotency-Key"))
			}
			var body struct {
				Payload struct {
					BodyBase64 string `json:"body_base64"`
				} `json:"payload"`
			}
			if err := json.NewDecoder(request.Body).Decode(&body); err != nil {
				t.Error(err)
			}
			payload, err := base64.StdEncoding.DecodeString(body.Payload.BodyBase64)
			if err != nil || len(payload) != 17 {
				t.Errorf("payload bytes = %d, err=%v", len(payload), err)
			}
			writeTestJSON(w, http.StatusAccepted, map[string]any{"id": id})
		case request.Method == http.MethodPatch && strings.HasSuffix(request.URL.Path, "/schedule"):
			writeTestJSON(w, http.StatusOK, map[string]any{"id": id})
		case request.Method == http.MethodDelete:
			writeTestJSON(w, http.StatusOK, map[string]any{"id": id})
		case request.Method == http.MethodGet:
			writeTestJSON(w, http.StatusOK, map[string]any{
				"id": id, "status": "delivered", "attempts": 2, "last_error": "retry",
			})
		default:
			http.NotFound(w, request)
		}
	}))
	defer server.Close()

	client, err := NewGatewayClient(
		GatewayConfig{URL: server.URL + "/", Timeout: Duration(time.Second)}, 2, 2,
	)
	if err != nil {
		t.Fatal(err)
	}
	planned := PlannedMessage{DeliverAt: time.Now().Add(time.Minute), PayloadBytes: 17, MaxAttempts: 3}
	gotID, err := client.Create(t.Context(), planned, "http://receiver/hook", "load-1")
	if err != nil || gotID != id {
		t.Fatalf("Create() id=%q err=%v", gotID, err)
	}
	if err := client.Reschedule(t.Context(), id, time.Now().Add(2*time.Minute)); err != nil {
		t.Fatal(err)
	}
	if err := client.Cancel(t.Context(), id); err != nil {
		t.Fatal(err)
	}
	status, err := client.Status(t.Context(), id)
	if err != nil {
		t.Fatal(err)
	}
	if status.Status != "delivered" || status.Attempts != 2 || status.LastError != "retry" {
		t.Fatalf("Status() = %+v", status)
	}
	if len(calls) != 4 {
		t.Fatalf("Gateway calls = %v", calls)
	}
}

func TestGatewayClientBoundsCreateConcurrency(t *testing.T) {
	var active atomic.Int32
	var maximum atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		current := active.Add(1)
		defer active.Add(-1)
		for {
			previous := maximum.Load()
			if current <= previous || maximum.CompareAndSwap(previous, current) {
				break
			}
		}
		time.Sleep(30 * time.Millisecond)
		writeTestJSON(w, http.StatusAccepted, map[string]any{"id": "0198f4f4-1b9e-7d3e-9c65-96c7af740db1"})
	}))
	defer server.Close()
	client, err := NewGatewayClient(
		GatewayConfig{URL: server.URL, Timeout: Duration(time.Second)}, 1, 1,
	)
	if err != nil {
		t.Fatal(err)
	}
	planned := PlannedMessage{DeliverAt: time.Now().Add(time.Minute), MaxAttempts: 1}
	errs := make(chan error, 3)
	for index := range 3 {
		go func() {
			_, err := client.Create(t.Context(), planned, "http://receiver/hook", string(rune('a'+index)))
			errs <- err
		}()
	}
	for range 3 {
		if err := <-errs; err != nil {
			t.Fatal(err)
		}
	}
	if got := maximum.Load(); got != 1 {
		t.Fatalf("maximum concurrent creates = %d, want 1", got)
	}
}

func writeTestJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}
