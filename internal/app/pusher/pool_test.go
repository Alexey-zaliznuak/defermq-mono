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

	jobs := pool.claimMessages(context.Background(), context.Background(), []Message{message})

	if repository.claimCalls != 0 {
		t.Fatalf("claim calls = %d, want 0", repository.claimCalls)
	}
	if len(jobs) != 0 {
		t.Fatalf("jobs = %d, want 0", len(jobs))
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

	jobs := pool.claimMessages(context.Background(), context.Background(), []Message{message})
	if len(jobs) != 1 {
		t.Fatalf("claimed jobs = %d, want 1", len(jobs))
	}
	pool.handleClaimed(context.Background(), 0, jobs[0])

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

	jobs := pool.claimMessages(context.Background(), context.Background(), []Message{message})
	if len(jobs) != 1 {
		t.Fatalf("claimed jobs = %d, want 1", len(jobs))
	}
	pool.handleClaimed(context.Background(), 0, jobs[0])

	if !repository.retried || repository.dead {
		t.Fatalf("retry=%v dead=%v", repository.retried, repository.dead)
	}
	if !message.acked {
		t.Fatal("message was not ACKed after retry commit")
	}
}

func TestClaimMessagesBatchesAndRoutesOutcomes(t *testing.T) {
	deliverAt := time.Now().UTC().Add(-time.Second)
	claimed := successfulRepository(t, deliverAt).claim
	repository := &fakeRepository{claims: []ClaimResult{
		claimed,
		{Reason: ClaimStale},
		{Reason: ClaimTooEarly, Wait: 250 * time.Millisecond},
		{Reason: ClaimTerminal},
	}}
	pool := newTestPool(t, repository, nil)
	messages := []*fakeMessage{
		eventMessage(t, deliverAt),
		eventMessage(t, deliverAt),
		eventMessage(t, deliverAt),
		eventMessage(t, deliverAt),
	}
	input := make([]Message, len(messages))
	for index := range messages {
		input[index] = messages[index]
	}

	jobs := pool.claimMessages(context.Background(), context.Background(), input)

	if repository.claimCalls != 1 || len(repository.lastRequests) != len(messages) {
		t.Fatalf("batch calls=%d requests=%d", repository.claimCalls, len(repository.lastRequests))
	}
	if len(jobs) != 1 || jobs[0].message != messages[0] {
		t.Fatalf("claimed jobs = %#v", jobs)
	}
	if !messages[1].acked || !messages[3].acked {
		t.Fatal("stale and terminal events were not ACKed")
	}
	if messages[2].nakDelay != 250*time.Millisecond {
		t.Fatalf("too-early NAK delay = %s", messages[2].nakDelay)
	}
	if messages[0].acked || messages[0].nakDelay != 0 {
		t.Fatal("claimed event was acknowledged before worker transition")
	}
}

func TestClaimMessagesNAKsWholeBatchOnError(t *testing.T) {
	repository := &fakeRepository{claimError: errors.New("database unavailable")}
	pool := newTestPool(t, repository, nil)
	deliverAt := time.Now().UTC().Add(-time.Second)
	first := eventMessage(t, deliverAt)
	second := eventMessage(t, deliverAt)

	jobs := pool.claimMessages(
		context.Background(), context.Background(), []Message{first, second},
	)

	if len(jobs) != 0 || repository.claimCalls != 1 {
		t.Fatalf("jobs=%d claim calls=%d", len(jobs), repository.claimCalls)
	}
	if first.nakDelay != time.Second || second.nakDelay != time.Second {
		t.Fatalf("batch NAK delays = %s, %s", first.nakDelay, second.nakDelay)
	}
}

func TestRunCollectsMessagesUpToFetchBatchSize(t *testing.T) {
	deliverAt := time.Now().UTC().Add(-time.Second)
	messages := []*fakeMessage{
		eventMessage(t, deliverAt),
		eventMessage(t, deliverAt),
		eventMessage(t, deliverAt),
		eventMessage(t, deliverAt),
	}
	input := make([]Message, len(messages))
	for index := range messages {
		input[index] = messages[index]
	}
	consumer := &batchConsumer{messages: input}
	ctx, cancel := context.WithCancel(context.Background())
	repository := &fakeRepository{
		claims: []ClaimResult{
			{Reason: ClaimStale},
			{Reason: ClaimStale},
			{Reason: ClaimStale},
		},
		onClaim: func([]ClaimRequest) { cancel() },
	}
	dispatcher, err := delivery.NewDispatcher(&fakeAdapter{})
	if err != nil {
		t.Fatal(err)
	}
	pool, err := NewPool(PoolConfig{
		Workers:            1,
		QueueSize:          3,
		FetchBatchSize:     3,
		FetchMaxWait:       time.Second,
		ClaimBatchSize:     10,
		ClaimFlushInterval: time.Second,
		ProcessingLease:    2 * time.Hour,
		HeartbeatInterval:  time.Hour,
		ClockSkewTolerance: 10 * time.Millisecond,
		MaxPayloadBytes:    1024,
		HotHorizon:         time.Minute,
		TransitionRetry:    time.Second,
		ShutdownTimeout:    time.Second,
	}, "test-owner", consumer, repository, dispatcher, delivery.Backoff{
		Initial: time.Second, Multiplier: 2, Max: time.Minute, Jitter: delivery.JitterNone,
	}, nil)
	if err != nil {
		t.Fatal(err)
	}

	if err := pool.Run(ctx); err != nil {
		t.Fatal(err)
	}

	if repository.claimCalls != 1 || len(repository.lastRequests) != 3 {
		t.Fatalf("batch calls=%d requests=%d, want 1 call with 3 requests",
			repository.claimCalls, len(repository.lastRequests))
	}
	for index := 0; index < 3; index++ {
		if !messages[index].acked {
			t.Errorf("message %d was not ACKed", index)
		}
	}
	if messages[3].acked || messages[3].termed || messages[3].nakDelay != 0 {
		t.Fatal("message beyond fetch batch limit was consumed")
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
		ClaimBatchSize:     100,
		ClaimFlushInterval: 10 * time.Millisecond,
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
		}, Payload: &domain.Payload{
			Body: []byte("body"), ContentType: "text/plain", SizeBytes: 4,
		}},
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
func (fakeConsumer) Next(context.Context) (Message, error) {
	return nil, nil
}
func (fakeConsumer) Ready(context.Context) error { return nil }
func (fakeConsumer) Close(context.Context) error { return nil }

type batchConsumer struct {
	messages []Message
	next     int
}

func (*batchConsumer) Type() domain.DestinationType { return domain.DestinationHTTP }
func (c *batchConsumer) Next(ctx context.Context) (Message, error) {
	if c.next < len(c.messages) {
		message := c.messages[c.next]
		c.next++
		return message, nil
	}
	<-ctx.Done()
	return nil, ctx.Err()
}
func (*batchConsumer) Ready(context.Context) error { return nil }
func (*batchConsumer) Close(context.Context) error { return nil }

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
	claimCalls   int
	claim        ClaimResult
	claims       []ClaimResult
	claimError   error
	onClaim      func([]ClaimRequest)
	lastRequests []ClaimRequest
	delivered    bool
	retried      bool
	dead         bool
}

func (r *fakeRepository) ClaimBatch(
	_ context.Context,
	requests []ClaimRequest,
	_ string,
	_ time.Duration,
	_ time.Duration,
) ([]ClaimResult, error) {
	r.claimCalls++
	r.lastRequests = append([]ClaimRequest(nil), requests...)
	if r.onClaim != nil {
		r.onClaim(requests)
	}
	if r.claimError != nil {
		return nil, r.claimError
	}
	if r.claims != nil {
		return r.claims, nil
	}
	results := make([]ClaimResult, len(requests))
	for index := range results {
		results[index] = r.claim
	}
	return results, nil
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
