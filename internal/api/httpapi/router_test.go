package httpapi_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/defermq/defermq/internal/api/httpapi"
	"github.com/defermq/defermq/internal/app/gateway"
	"github.com/defermq/defermq/internal/domain"
	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
)

type fakeRepository struct {
	mu          sync.Mutex
	messages    map[uuid.UUID]gateway.Message
	keys        map[string]uuid.UUID
	pingErr     error
	schemaErr   error
	panicCreate bool
	waitCreate  bool
}

func newFakeRepository() *fakeRepository {
	return &fakeRepository{messages: make(map[uuid.UUID]gateway.Message), keys: make(map[string]uuid.UUID)}
}

func (f *fakeRepository) Create(ctx context.Context, command gateway.CreateCommand) (domain.Delivery, bool, error) {
	if f.panicCreate {
		panic("test panic")
	}
	if f.waitCreate {
		<-ctx.Done()
		return domain.Delivery{}, false, ctx.Err()
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if command.IdempotencyKey != nil {
		if id, exists := f.keys[*command.IdempotencyKey]; exists {
			return f.messages[id].Delivery, true, nil
		}
	}
	now := time.Date(2026, 7, 25, 12, 0, 0, 0, time.UTC)
	delivery := domain.Delivery{
		ID: uuid.New(), PayloadID: command.Payload.ID, IdempotencyKey: command.IdempotencyKey,
		DestinationType: command.DestinationType, Destination: command.Destination,
		DeliverAt: command.DeliverAt, Status: domain.StatusScheduled, ScheduleRevision: 1,
		MaxAttempts: command.MaxAttempts, CreatedAt: now, UpdatedAt: now,
	}
	f.messages[delivery.ID] = gateway.Message{
		Delivery: delivery,
		Payload: gateway.PayloadMetadata{
			ID: command.Payload.ID, ContentType: command.Payload.ContentType,
			SizeBytes: int64(len(command.Payload.Body)), CreatedAt: now,
		},
	}
	if command.IdempotencyKey != nil {
		f.keys[*command.IdempotencyKey] = delivery.ID
	}
	return delivery, false, nil
}

func (f *fakeRepository) Get(_ context.Context, id uuid.UUID) (gateway.Message, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	message, exists := f.messages[id]
	if !exists {
		return gateway.Message{}, domain.ErrNotFound
	}
	return message, nil
}

func (f *fakeRepository) Reschedule(_ context.Context, id uuid.UUID, at time.Time, _ time.Duration) (domain.Delivery, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	message, exists := f.messages[id]
	if !exists {
		return domain.Delivery{}, domain.ErrNotFound
	}
	if message.Delivery.Status != domain.StatusScheduled {
		return domain.Delivery{}, domain.ErrInvalidState
	}
	message.Delivery.DeliverAt = at.UTC()
	message.Delivery.ScheduleRevision++
	message.Delivery.HotRegisteredRevision = nil
	f.messages[id] = message
	return message.Delivery, nil
}

func (f *fakeRepository) Cancel(_ context.Context, id uuid.UUID) (domain.Delivery, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	message, exists := f.messages[id]
	if !exists {
		return domain.Delivery{}, domain.ErrNotFound
	}
	if message.Delivery.Status != domain.StatusScheduled {
		return domain.Delivery{}, domain.ErrInvalidState
	}
	now := time.Now().UTC()
	message.Delivery.Status = domain.StatusCancelled
	message.Delivery.ScheduleRevision++
	message.Delivery.CancelledAt = &now
	f.messages[id] = message
	return message.Delivery, nil
}

func (f *fakeRepository) Ping(context.Context) error        { return f.pingErr }
func (f *fakeRepository) CheckSchema(context.Context) error { return f.schemaErr }

func newTestHandler(t *testing.T, repository *fakeRepository, payloadLimit, bodyLimit int64) http.Handler {
	t.Helper()
	service, err := gateway.New(repository, gateway.Options{
		HotHorizon: 2 * time.Minute, MaxPayloadBytes: payloadLimit, DefaultMaxAttempts: 10,
		MaxIdempotencyKeyBytes: 200,
		EnabledDestinations:    map[domain.DestinationType]bool{domain.DestinationHTTP: true},
	})
	if err != nil {
		t.Fatal(err)
	}
	registry := prometheus.NewRegistry()
	handler, err := httpapi.NewRouter(httpapi.Options{
		Service: service, Logger: zap.NewNop(), Registerer: registry, Gatherer: registry,
		RequestTimeout: time.Second, MaxBodyBytes: bodyLimit, ServiceName: "test", Version: "dev",
	})
	if err != nil {
		t.Fatal(err)
	}
	return handler
}

func request(t *testing.T, handler http.Handler, method, path, body string, headers map[string]string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	for key, value := range headers {
		req.Header.Set(key, value)
	}
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, req)
	return response
}

func TestMessageLifecycleAndIdempotencyReplay(t *testing.T) {
	repository := newFakeRepository()
	handler := newTestHandler(t, repository, 1024, 4096)
	createBody := `{
		"deliver_at":"2026-07-26T10:00:00+03:00",
		"destination":{"type":"http","http":{"url":"https://example.com/hook"}},
		"payload":{"content_type":"application/json","body":{"event":"created"}}
	}`
	created := request(t, handler, http.MethodPost, "/v1/messages", createBody, map[string]string{"Idempotency-Key": "event-1"})
	if created.Code != http.StatusAccepted {
		t.Fatalf("create status = %d, body=%s", created.Code, created.Body.String())
	}
	var createResponse struct {
		ID uuid.UUID `json:"id"`
	}
	if err := json.NewDecoder(created.Body).Decode(&createResponse); err != nil {
		t.Fatal(err)
	}

	replay := request(t, handler, http.MethodPost, "/v1/messages", createBody, map[string]string{"Idempotency-Key": "event-1"})
	if replay.Code != http.StatusAccepted || replay.Header().Get("Idempotent-Replay") != "true" {
		t.Fatalf("replay status=%d replay=%q", replay.Code, replay.Header().Get("Idempotent-Replay"))
	}

	got := request(t, handler, http.MethodGet, "/v1/messages/"+createResponse.ID.String(), "", nil)
	if got.Code != http.StatusOK || bytes.Contains(got.Body.Bytes(), []byte(`"body"`)) {
		t.Fatalf("get status=%d body=%s", got.Code, got.Body.String())
	}
	if !bytes.Contains(got.Body.Bytes(), []byte(`"content_type":"application/json"`)) {
		t.Fatalf("payload metadata missing: %s", got.Body.String())
	}

	rescheduled := request(t, handler, http.MethodPatch, "/v1/messages/"+createResponse.ID.String()+"/schedule",
		`{"deliver_at":"2026-07-27T10:00:00Z"}`, nil)
	if rescheduled.Code != http.StatusOK || !bytes.Contains(rescheduled.Body.Bytes(), []byte(`"schedule_revision":2`)) {
		t.Fatalf("reschedule status=%d body=%s", rescheduled.Code, rescheduled.Body.String())
	}

	cancelled := request(t, handler, http.MethodDelete, "/v1/messages/"+createResponse.ID.String(), "", nil)
	if cancelled.Code != http.StatusOK || !bytes.Contains(cancelled.Body.Bytes(), []byte(`"status":"cancelled"`)) {
		t.Fatalf("cancel status=%d body=%s", cancelled.Code, cancelled.Body.String())
	}
}

func TestValidationBase64AndLimits(t *testing.T) {
	handler := newTestHandler(t, newFakeRepository(), 3, 512)
	valid := `{"deliver_at":"2026-07-26T10:00:00Z","destination":{"type":"http","http":{"url":"https://example.com"}},"payload":{"content_type":"application/octet-stream","body_base64":"AAEC"}}`
	if response := request(t, handler, http.MethodPost, "/v1/messages", valid, nil); response.Code != http.StatusAccepted {
		t.Fatalf("base64 status=%d body=%s", response.Code, response.Body.String())
	}
	invalidUnion := strings.Replace(valid, `"body_base64":"AAEC"`, `"body":{},"body_base64":"AAEC"`, 1)
	if response := request(t, handler, http.MethodPost, "/v1/messages", invalidUnion, nil); response.Code != http.StatusBadRequest {
		t.Fatalf("union status=%d body=%s", response.Code, response.Body.String())
	}
	tooLargePayload := strings.Replace(valid, "AAEC", "AAECAw==", 1)
	if response := request(t, handler, http.MethodPost, "/v1/messages", tooLargePayload, nil); response.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("payload limit status=%d body=%s", response.Code, response.Body.String())
	}
	huge := `{"padding":"` + strings.Repeat("x", 600) + `"}`
	if response := request(t, handler, http.MethodPost, "/v1/messages", huge, nil); response.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("body limit status=%d body=%s", response.Code, response.Body.String())
	}
}

func TestDestinationValidationAndHealth(t *testing.T) {
	repository := newFakeRepository()
	handler := newTestHandler(t, repository, 1024, 4096)
	invalid := `{"deliver_at":"2026-07-26T10:00:00Z","destination":{"type":"http","http":{"url":"ftp://example.com"}},"payload":{"content_type":"application/json","body":{}}}`
	if response := request(t, handler, http.MethodPost, "/v1/messages", invalid, nil); response.Code != http.StatusBadRequest {
		t.Fatalf("destination status=%d body=%s", response.Code, response.Body.String())
	}
	if response := request(t, handler, http.MethodGet, "/livez", "", nil); response.Code != http.StatusOK {
		t.Fatalf("live status=%d", response.Code)
	}
	repository.schemaErr = errors.New("missing table")
	if response := request(t, handler, http.MethodGet, "/readyz", "", nil); response.Code != http.StatusServiceUnavailable {
		t.Fatalf("ready status=%d body=%s", response.Code, response.Body.String())
	}
	if response := request(t, handler, http.MethodGet, "/metrics", "", nil); response.Code != http.StatusOK {
		t.Fatalf("metrics status=%d", response.Code)
	}
}

func TestMiddlewareTimeoutRecovererAndContentType(t *testing.T) {
	repository := newFakeRepository()
	handler := newTestHandler(t, repository, 1024, 4096)
	body := `{"deliver_at":"2026-07-26T10:00:00Z","destination":{"type":"http","http":{"url":"https://example.com"}},"payload":{"content_type":"application/json","body":{}}}`

	req := httptest.NewRequest(http.MethodPost, "/v1/messages", strings.NewReader(body))
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, req)
	if response.Code != http.StatusUnsupportedMediaType {
		t.Fatalf("content type status=%d body=%s", response.Code, response.Body.String())
	}

	repository.panicCreate = true
	response = request(t, handler, http.MethodPost, "/v1/messages", body, nil)
	if response.Code != http.StatusInternalServerError || response.Header().Get("X-Request-ID") == "" {
		t.Fatalf("panic status=%d request-id=%q body=%s", response.Code, response.Header().Get("X-Request-ID"), response.Body.String())
	}

	repository.panicCreate = false
	repository.waitCreate = true
	response = request(t, handler, http.MethodPost, "/v1/messages", body, nil)
	if response.Code != http.StatusGatewayTimeout {
		t.Fatalf("timeout status=%d body=%s", response.Code, response.Body.String())
	}
}
