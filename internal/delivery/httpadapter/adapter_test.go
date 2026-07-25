package httpadapter

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/defermq/defermq/internal/delivery"
	"github.com/defermq/defermq/internal/domain"
	"github.com/google/uuid"
)

func TestPushAddsSystemHeadersAndPreservesPayload(t *testing.T) {
	id := uuid.New()
	payload := []byte{0, 1, 2, 255}
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		body, _ := io.ReadAll(request.Body)
		if string(body) != string(payload) {
			t.Errorf("body = %v, want %v", body, payload)
		}
		if got := request.Header.Get("Idempotency-Key"); got != id.String() {
			t.Errorf("Idempotency-Key = %q, want %q", got, id)
		}
		if got := request.Header.Get("X-DeferMQ-Attempt"); got != "2" {
			t.Errorf("attempt header = %q", got)
		}
		writer.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	adapter, err := New(testConfig(true))
	if err != nil {
		t.Fatal(err)
	}
	err = adapter.Push(context.Background(), delivery.PushRequest{
		DeliveryID:       id,
		ScheduleRevision: 3,
		Attempt:          2,
		ScheduledAt:      time.Now(),
		Destination: domain.Destination{
			Type: domain.DestinationHTTP,
			HTTP: &domain.HTTPDestination{
				URL:     server.URL,
				Method:  http.MethodPost,
				Headers: map[string]string{"Idempotency-Key": "user-value"},
			},
		},
		Payload:     payload,
		ContentType: "application/octet-stream",
	})
	if err != nil {
		t.Fatalf("Push() error = %v", err)
	}
}

func TestPushClassifiesStatuses(t *testing.T) {
	tests := []struct {
		status    int
		retryable bool
	}{
		{http.StatusBadRequest, false},
		{http.StatusRequestTimeout, true},
		{http.StatusTooManyRequests, true},
		{http.StatusServiceUnavailable, true},
	}
	for _, test := range tests {
		t.Run(http.StatusText(test.status), func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
				writer.Header().Set("Retry-After", "7")
				http.Error(writer, "diagnostic", test.status)
			}))
			defer server.Close()
			adapter, _ := New(testConfig(true))
			err := adapter.Push(context.Background(), delivery.PushRequest{
				DeliveryID: uuid.New(),
				Destination: domain.Destination{
					Type: domain.DestinationHTTP,
					HTTP: &domain.HTTPDestination{URL: server.URL},
				},
			})
			var pushErr *delivery.PushError
			if !errors.As(err, &pushErr) {
				t.Fatalf("Push() error = %v, want PushError", err)
			}
			if pushErr.Retryable != test.retryable {
				t.Fatalf("Retryable = %v, want %v", pushErr.Retryable, test.retryable)
			}
			if test.status == http.StatusTooManyRequests && pushErr.RetryAfter != 7*time.Second {
				t.Fatalf("RetryAfter = %s", pushErr.RetryAfter)
			}
		})
	}
}

func TestPushBlocksPrivateAddress(t *testing.T) {
	adapter, err := New(testConfig(false))
	if err != nil {
		t.Fatal(err)
	}
	err = adapter.Push(context.Background(), delivery.PushRequest{
		DeliveryID: uuid.New(),
		Destination: domain.Destination{
			Type: domain.DestinationHTTP,
			HTTP: &domain.HTTPDestination{URL: "http://127.0.0.1/hook"},
		},
	})
	var pushErr *delivery.PushError
	if !errors.As(err, &pushErr) || pushErr.Code != "ssrf_blocked" || pushErr.Retryable {
		t.Fatalf("Push() error = %#v, want non-retryable ssrf_blocked", err)
	}
}

func testConfig(allowPrivate bool) Config {
	return Config{
		Timeout:               time.Second,
		DialTimeout:           time.Second,
		TLSHandshakeTimeout:   time.Second,
		ResponseHeaderTimeout: time.Second,
		IdleConnTimeout:       time.Second,
		MaxIdleConns:          10,
		MaxIdleConnsPerHost:   2,
		MaxResponseBodyBytes:  1024,
		AllowPrivateNetworks:  allowPrivate,
	}
}
