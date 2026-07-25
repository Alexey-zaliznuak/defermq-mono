package pusher

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/defermq/defermq/internal/delivery"
	"github.com/defermq/defermq/internal/domain"
	"github.com/defermq/defermq/internal/hotstorage/natsjs"
	"github.com/defermq/defermq/internal/observability"
	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
)

func TestHandleDoesNotClaimEarlyEvent(t *testing.T) {
	now := time.Date(2026, 7, 25, 12, 0, 0, 0, time.UTC)
	repository := &fakeRepository{}
	pool := newTestPool(t, repository, nil)
	pool.now = func() time.Time { return now }
	message := eventMessage(t, now.Add(time.Minute))

	pool.handle(context.Background(), 0, message)

	if repository.claimCalls != 0 {
		t.Fatalf("claim calls = %d, want 0", repository.claimCalls)
	}
	if message.nakDelay != time.Minute || message.acked || message.termed {
		t.Fatalf("message state: ack=%v term=%v nak=%s", message.acked, message.termed, message.nakDelay)
	}
}

func TestHandleACKsOnlyAfterSuccessCommit(t *testing.T) {
	now := time.Now().UTC().Add(-time.Second)
	repository := successfulRepository(t, now)
	adapter := &fakeAdapter{}
	pool := newTestPool(t, repository, adapter)
	pool.SetMetrics(observability.NewPusherMetrics(prometheus.NewRegistry()))
	message := eventMessage(t, now)
	message.committed = func() bool { return repository.delivered }

	pool.handle(context.Background(), 0, message)

	if !repository.delivered {
		t.Fatal("delivery was not committed")
	}
	if !message.acked || !message.commitObserved {
		t.Fatalf("ACK state: acked=%v commitObserved=%v", message.acked, message.commitObserved)
	}
}

func TestHandleSchedulesRetryForTypedError(t *testing.T) {
	now := time.Now().UTC().Add(-time.Second)
	repository := successfulRepository(t, now)
	adapter := &fakeAdapter{err: delivery.NewPushError("temporary", true, errors.New("unavailable"))}
	pool := newTestPool(t, repository, adapter)
	message := eventMessage(t, now)

	pool.handle(context.Background(), 0, message)

	if !repository.retried || repository.dead {
		t.Fatalf("retry=%v dead=%v", repository.retried, repository.dead)
	}
	if !message.acked {
		t.Fatal("message was not ACKed after retry commit")
	}
}

func TestDecodeEventRejectsUnknownFields(t *testing.T) {
	data := []byte(`{"schema_version":1,"delivery_id":"` + uuid.NewString() +
		`","schedule_revision":1,"deliver_at":"2026-07-25T12:00:00Z","destination_type":"http","extra":true}`)
	if _, err := decodeEvent(data, domain.DestinationHTTP); err == nil {
		t.Fatal("decodeEvent() unexpectedly accepted an unknown field")
	}
}

func newTestPool(t *testing.T, repository *fakeRepository, adapter *fakeAdapter) *Pool {
	t.Helper()
	if adapter == nil {
		adapter = &fakeAdapter{}
	}
	dispatcher, err := delivery.NewDispatcher(adapter)
	if err != nil {
		t.Fatal(err)
	}
	pool, err := NewPool(PoolConfig{
		Workers:            1,
		QueueSize:          1,
		FetchBatchSize:     1,
		FetchMaxWait:       time.Second,
		ProcessingLease:    2 * time.Hour,
		HeartbeatInterval:  time.Hour,
		ClockSkewTolerance: 10 * time.Millisecond,
		MaxPayloadBytes:    1024,
		HotHorizon:         time.Minute,
		TransitionRetry:    time.Second,
		ShutdownTimeout:    time.Second,
	}, "test-owner", fakeConsumer{}, repository, dispatcher, delivery.Backoff{
		Initial: time.Second, Multiplier: 2, Max: time.Minute, Jitter: delivery.JitterNone,
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	return pool
}

func successfulRepository(t *testing.T, scheduledAt time.Time) *fakeRepository {
	t.Helper()
	destination, err := json.Marshal(domain.Destination{
		Type: domain.DestinationHTTP,
		HTTP: &domain.HTTPDestination{URL: "https://example.com"},
	})
	if err != nil {
		t.Fatal(err)
	}
	return &fakeRepository{
		claim: ClaimResult{Reason: Claimed, Delivery: &domain.Delivery{
			ID:               uuid.New(),
			PayloadID:        uuid.New(),
			DestinationType:  domain.DestinationHTTP,
			Destination:      destination,
			DeliverAt:        scheduledAt,
			ScheduleRevision: 1,
			Attempts:         1,
			MaxAttempts:      3,
		}},
		payload: domain.Payload{Body: []byte("body"), ContentType: "text/plain"},
	}
}

func eventMessage(t *testing.T, deliverAt time.Time) *fakeMessage {
	t.Helper()
	event := natsjs.ReadyEvent{
		SchemaVersion:    natsjs.EventSchemaVersion,
		DeliveryID:       uuid.New(),
		ScheduleRevision: 1,
		DeliverAt:        deliverAt,
		DestinationType:  domain.DestinationHTTP,
	}
	data, err := json.Marshal(event)
	if err != nil {
		t.Fatal(err)
	}
	return &fakeMessage{data: data}
}

type fakeConsumer struct{}

func (fakeConsumer) Type() domain.DestinationType { return domain.DestinationHTTP }
func (fakeConsumer) Fetch(context.Context, int, time.Duration) ([]Message, error) {
	return nil, nil
}
func (fakeConsumer) Ready(context.Context) error { return nil }
func (fakeConsumer) Close(context.Context) error { return nil }

type fakeMessage struct {
	data           []byte
	acked          bool
	termed         bool
	nakDelay       time.Duration
	committed      func() bool
	commitObserved bool
}

func (m *fakeMessage) Data() []byte { return m.data }
func (m *fakeMessage) Ack(context.Context) error {
	m.acked = true
	if m.committed != nil {
		m.commitObserved = m.committed()
	}
	return nil
}
func (m *fakeMessage) Nak(_ context.Context, delay time.Duration) error {
	m.nakDelay = delay
	return nil
}
func (m *fakeMessage) Term(context.Context) error {
	m.termed = true
	return nil
}

type fakeAdapter struct {
	err error
}

func (a *fakeAdapter) Type() domain.DestinationType { return domain.DestinationHTTP }
func (a *fakeAdapter) Push(context.Context, delivery.PushRequest) error {
	return a.err
}
func (a *fakeAdapter) Ready(context.Context) error { return nil }
func (a *fakeAdapter) Close(context.Context) error { return nil }

type fakeRepository struct {
	claimCalls int
	claim      ClaimResult
	payload    domain.Payload
	delivered  bool
	retried    bool
	dead       bool
}

func (r *fakeRepository) Claim(context.Context, uuid.UUID, int64, string, time.Duration, time.Duration) (ClaimResult, error) {
	r.claimCalls++
	return r.claim, nil
}
func (r *fakeRepository) LoadPayload(context.Context, uuid.UUID, int64) (domain.Payload, error) {
	return r.payload, nil
}
func (r *fakeRepository) Heartbeat(context.Context, uuid.UUID, string, time.Duration) (bool, error) {
	return true, nil
}
func (r *fakeRepository) MarkDelivered(context.Context, uuid.UUID, string) (bool, error) {
	r.delivered = true
	return true, nil
}
func (r *fakeRepository) ScheduleRetry(context.Context, uuid.UUID, string, time.Duration, string, time.Duration) (bool, error) {
	r.retried = true
	return true, nil
}
func (r *fakeRepository) MarkDead(context.Context, uuid.UUID, string, string) (bool, error) {
	r.dead = true
	return true, nil
}
func (r *fakeRepository) Ready(context.Context) error { return nil }
