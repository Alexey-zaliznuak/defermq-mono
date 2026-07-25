package natsjs

import (
	"context"
	"testing"
	"time"

	"github.com/defermq/defermq/internal/domain"
	"github.com/google/uuid"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

type recordingPublisher struct {
	message *nats.Msg
}

func (p *recordingPublisher) PublishMsg(_ context.Context, msg *nats.Msg, _ ...jetstream.PublishOpt) (*jetstream.PubAck, error) {
	p.message = msg
	return &jetstream.PubAck{Stream: "DEFERMQ", Sequence: 1}, nil
}

func TestPublisherPublishesOnlyReadySubject(t *testing.T) {
	now := time.Date(2026, 7, 25, 12, 0, 0, 123, time.UTC)
	id := uuid.New()
	recorder := &recordingPublisher{}
	publisher := NewPublisher(recorder, Subjects{"defermq.schedule", "defermq.ready"})
	publisher.now = func() time.Time { return now }
	request := PublishRequest{
		Kind:             OutboxReady,
		DeliveryID:       id,
		ScheduleRevision: 3,
		DeliverAt:        now.Add(time.Minute),
		DestinationType:  domain.DestinationHTTP,
	}
	if err := publisher.Publish(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	if recorder.message.Subject != "defermq.ready.http" {
		t.Fatalf("unexpected ready subject %q", recorder.message.Subject)
	}

	request.Kind = OutboxHotRegister
	_, err := publisher.Decide(request, now)
	if err == nil {
		t.Fatal("hot-register outbox was accepted by NATS publisher")
	}
	request.Kind = OutboxReady
	decision, err := publisher.Decide(request, now)
	if err != nil {
		t.Fatal(err)
	}
	if decision.Scheduled || decision.Subject != "defermq.ready.http" {
		t.Fatalf("ready decision is invalid: %+v", decision)
	}
}

func TestDecodeReadyEventRejectsDestination(t *testing.T) {
	data := []byte(`{"schema_version":1,"delivery_id":"018f6544-7c00-7000-8000-000000000001","schedule_revision":1,"deliver_at":"2026-07-25T12:00:00Z","destination_type":"kafka"}`)
	if _, err := DecodeReadyEvent(data, domain.DestinationHTTP); err == nil {
		t.Fatal("destination mismatch accepted")
	}
}
