package integration_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"sync"
	"testing"
	"time"
)

type messageStatus struct {
	ID        string `json:"id"`
	Status    string `json:"status"`
	Attempts  int    `json:"attempts"`
	DeliverAt string `json:"deliver_at"`
}

func TestHTTPDeliveryEndToEnd(t *testing.T) {
	gatewayURL := os.Getenv("DEFERMQ_E2E_GATEWAY_URL")
	if gatewayURL == "" {
		t.Skip("set DEFERMQ_E2E_GATEWAY_URL for a running local DeferMQ stack")
	}

	var mu sync.Mutex
	var receivedAt time.Time
	var attempts int
	var deliveryID string
	receiver := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		attempts++
		receivedAt = time.Now()
		deliveryID = r.Header.Get("X-DeferMQ-Delivery-ID")
		if attempts == 1 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer receiver.Close()

	deliverAt := time.Now().UTC().Add(2 * time.Second)
	id := createMessage(t, gatewayURL, receiver.URL, deliverAt)
	waitForStatus(t, gatewayURL, id, "delivered", 45*time.Second)

	mu.Lock()
	defer mu.Unlock()
	if attempts < 2 {
		t.Fatalf("receiver attempts = %d, want retry after initial 503", attempts)
	}
	if receivedAt.Before(deliverAt.Add(-100 * time.Millisecond)) {
		t.Fatalf("delivery arrived early: received=%s scheduled=%s", receivedAt, deliverAt)
	}
	if deliveryID != id {
		t.Fatalf("delivery header = %q, want %q", deliveryID, id)
	}
}

func TestCancellationEndToEnd(t *testing.T) {
	gatewayURL := os.Getenv("DEFERMQ_E2E_GATEWAY_URL")
	if gatewayURL == "" {
		t.Skip("set DEFERMQ_E2E_GATEWAY_URL for a running local DeferMQ stack")
	}
	delivered := make(chan struct{}, 1)
	receiver := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		delivered <- struct{}{}
	}))
	defer receiver.Close()

	deliverAt := time.Now().UTC().Add(2 * time.Second)
	id := createMessage(t, gatewayURL, receiver.URL, deliverAt)
	response := request(t, http.MethodDelete, gatewayURL+"/v1/messages/"+id, nil, http.StatusOK)
	_ = response.Close()
	waitForStatus(t, gatewayURL, id, "cancelled", 5*time.Second)
	select {
	case <-delivered:
		t.Fatal("cancelled message was delivered")
	case <-time.After(time.Until(deliverAt.Add(1500 * time.Millisecond))):
	}
}

func createMessage(t *testing.T, gatewayURL, destinationURL string, deliverAt time.Time) string {
	t.Helper()
	body := map[string]any{
		"deliver_at": deliverAt.Format(time.RFC3339Nano),
		"destination": map[string]any{
			"type": "http",
			"http": map[string]any{"url": destinationURL, "method": http.MethodPost},
		},
		"payload": map[string]any{
			"content_type": "application/json",
			"body":         map[string]any{"e2e": true},
		},
	}
	response := request(t, http.MethodPost, gatewayURL+"/v1/messages", body, http.StatusAccepted)
	var status messageStatus
	decode(t, response, &status)
	if status.ID == "" {
		t.Fatal("Gateway returned an empty delivery ID")
	}
	return status.ID
}

func waitForStatus(t *testing.T, gatewayURL, id, expected string, timeout time.Duration) messageStatus {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		response := request(t, http.MethodGet, gatewayURL+"/v1/messages/"+id, nil, http.StatusOK)
		var status messageStatus
		decode(t, response, &status)
		if status.Status == expected {
			return status
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("message %s did not reach status %q within %s", id, expected, timeout)
	return messageStatus{}
}

func request(t *testing.T, method, url string, body any, expectedStatus int) io.ReadCloser {
	t.Helper()
	var encoded io.Reader
	if body != nil {
		data, err := json.Marshal(body)
		if err != nil {
			t.Fatal(err)
		}
		encoded = bytes.NewReader(data)
	}
	req, err := http.NewRequest(method, url, encoded)
	if err != nil {
		t.Fatal(err)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	client := &http.Client{Timeout: 10 * time.Second}
	response, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != expectedStatus {
		defer response.Body.Close()
		data, _ := io.ReadAll(io.LimitReader(response.Body, 4096))
		t.Fatalf("%s %s returned %d, want %d: %s", method, url, response.StatusCode, expectedStatus, data)
	}
	return response.Body
}

func decode(t *testing.T, body io.ReadCloser, target any) {
	t.Helper()
	defer body.Close()
	if err := json.NewDecoder(body).Decode(target); err != nil {
		t.Fatal(fmt.Errorf("decode response: %w", err))
	}
}
