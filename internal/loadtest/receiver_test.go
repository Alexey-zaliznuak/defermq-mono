package loadtest

import (
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestReceiverRecordsRetryAndDuplicate(t *testing.T) {
	receiver := NewReceiver(ReceiverConfig{Path: "/hook"}, 100*time.Millisecond)
	id := uuid.NewString()
	receiver.Register(id, PlannedMessage{
		Group: "retry", FailFirstAttempts: 1, FailureStatus: http.StatusServiceUnavailable,
	})

	first := deliveryRequest(t, receiver, id, 1, time.Now().Add(-time.Second))
	if first.Code != http.StatusServiceUnavailable {
		t.Fatalf("first attempt status = %d, want 503", first.Code)
	}
	duplicate := deliveryRequest(t, receiver, id, 1, time.Now().Add(-time.Second))
	if duplicate.Code != http.StatusServiceUnavailable {
		t.Fatalf("duplicate attempt status = %d, want 503", duplicate.Code)
	}
	second := deliveryRequest(t, receiver, id, 2, time.Now().Add(-time.Second))
	if second.Code != http.StatusNoContent {
		t.Fatalf("second attempt status = %d, want 204", second.Code)
	}
	successDuplicate := deliveryRequest(t, receiver, id, 3, time.Now().Add(-time.Second))
	if successDuplicate.Code != http.StatusNoContent {
		t.Fatalf("successful duplicate status = %d, want 204", successDuplicate.Code)
	}

	observations := receiver.Observations()
	if len(observations) != 4 {
		t.Fatalf("observations = %d, want 4", len(observations))
	}
	if observations[0].Duplicate || observations[1].Duplicate || observations[2].Duplicate || !observations[3].Duplicate {
		t.Fatalf("unexpected duplicate flags: %#v", observations)
	}
	if observations[0].Lag <= 0 || observations[0].Early {
		t.Fatalf("first observation lag=%s early=%v", observations[0].Lag, observations[0].Early)
	}
}

func TestReceiverEarlyTolerance(t *testing.T) {
	receiver := NewReceiver(ReceiverConfig{Path: "/hook"}, 100*time.Millisecond)
	id := uuid.NewString()
	receiver.Register(id, PlannedMessage{Group: "timing"})

	withinTolerance := deliveryRequest(t, receiver, id, 1, time.Now().Add(50*time.Millisecond))
	if withinTolerance.Code != http.StatusNoContent {
		t.Fatalf("within-tolerance status = %d", withinTolerance.Code)
	}
	early := deliveryRequest(t, receiver, id, 2, time.Now().Add(500*time.Millisecond))
	if early.Code != http.StatusNoContent {
		t.Fatalf("early status = %d", early.Code)
	}
	observations := receiver.Observations()
	if observations[0].Early {
		t.Fatal("delivery inside early tolerance was marked early")
	}
	if !observations[1].Early || observations[1].Lag >= 0 {
		t.Fatalf("early observation lag=%s early=%v", observations[1].Lag, observations[1].Early)
	}
}

func TestReceiverRejectsInvalidSystemHeaders(t *testing.T) {
	receiver := NewReceiver(ReceiverConfig{Path: "/hook"}, 0)
	request := httptest.NewRequest(http.MethodPost, "/hook", nil)
	response := httptest.NewRecorder()
	receiver.Handler().ServeHTTP(response, request)
	if response.Code != http.StatusBadRequest {
		t.Fatalf("invalid-header status = %d, want 400", response.Code)
	}
	if len(receiver.Observations()) != 0 {
		t.Fatal("invalid request was recorded")
	}
}

func deliveryRequest(t *testing.T, receiver *Receiver, id string, attempt int, scheduledAt time.Time) *httptest.ResponseRecorder {
	t.Helper()
	request := httptest.NewRequest(http.MethodPost, "/hook", nil)
	request.Header.Set("Idempotency-Key", id)
	request.Header.Set("X-DeferMQ-Delivery-ID", id)
	request.Header.Set("X-DeferMQ-Schedule-Revision", "1")
	request.Header.Set("X-DeferMQ-Attempt", strconv.Itoa(attempt))
	request.Header.Set("X-DeferMQ-Scheduled-At", scheduledAt.UTC().Format(time.RFC3339Nano))
	response := httptest.NewRecorder()
	receiver.Handler().ServeHTTP(response, request)
	return response
}
