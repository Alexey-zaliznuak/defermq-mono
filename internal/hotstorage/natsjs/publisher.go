package natsjs

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/defermq/defermq/internal/domain"
	"github.com/google/uuid"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

type OutboxKind string

const (
	OutboxSchedule OutboxKind = "schedule"
	OutboxReady    OutboxKind = "ready"
)

type PublishRequest struct {
	Kind             OutboxKind
	DeliveryID       uuid.UUID
	ScheduleRevision int64
	DeliverAt        time.Time
	DestinationType  domain.DestinationType
}

type PublishDecision struct {
	Subject   string
	MessageID string
	Scheduled bool
	Target    string
}

type AckPublisher interface {
	PublishMsg(context.Context, *nats.Msg, ...jetstream.PublishOpt) (*jetstream.PubAck, error)
}

type Publisher struct {
	js       AckPublisher
	subjects Subjects
	now      func() time.Time
}

func NewPublisher(js AckPublisher, subjects Subjects) *Publisher {
	return &Publisher{js: js, subjects: subjects, now: time.Now}
}

func (p *Publisher) Decide(req PublishRequest, now time.Time) (PublishDecision, error) {
	if req.DeliveryID == uuid.Nil || req.ScheduleRevision <= 0 || req.DeliverAt.IsZero() {
		return PublishDecision{}, errors.New("invalid publish request")
	}
	if !validDestination(req.DestinationType) {
		return PublishDecision{}, fmt.Errorf("invalid destination type %q", req.DestinationType)
	}
	switch req.Kind {
	case OutboxReady:
		return PublishDecision{
			Subject:   p.subjects.Ready(req.DestinationType),
			MessageID: MessageID(req.DeliveryID, req.ScheduleRevision, string(OutboxReady)),
		}, nil
	case OutboxSchedule:
		if !req.DeliverAt.After(now) {
			return PublishDecision{
				Subject:   p.subjects.Ready(req.DestinationType),
				MessageID: MessageID(req.DeliveryID, req.ScheduleRevision, string(OutboxReady)),
			}, nil
		}
		return PublishDecision{
			Subject:   p.subjects.Schedule(req.DeliveryID),
			MessageID: MessageID(req.DeliveryID, req.ScheduleRevision, string(OutboxSchedule)),
			Scheduled: true,
			Target:    p.subjects.Ready(req.DestinationType),
		}, nil
	default:
		return PublishDecision{}, fmt.Errorf("invalid outbox kind %q", req.Kind)
	}
}

func (p *Publisher) Publish(ctx context.Context, req PublishRequest) error {
	decision, err := p.Decide(req, p.now().UTC())
	if err != nil {
		return err
	}
	event := ReadyEvent{
		SchemaVersion:    EventSchemaVersion,
		DeliveryID:       req.DeliveryID,
		ScheduleRevision: req.ScheduleRevision,
		DeliverAt:        req.DeliverAt.UTC(),
		DestinationType:  req.DestinationType,
	}
	body, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("encode ready event: %w", err)
	}
	msg := nats.NewMsg(decision.Subject)
	msg.Data = body
	if decision.Scheduled {
		msg.Header.Set(jetstream.ScheduleHeader, "@at "+req.DeliverAt.UTC().Format(time.RFC3339Nano))
		msg.Header.Set(jetstream.ScheduleTargetHeader, decision.Target)
	}
	ack, err := p.js.PublishMsg(ctx, msg, jetstream.WithMsgID(decision.MessageID))
	if err != nil {
		return fmt.Errorf("publish JetStream message: %w", err)
	}
	if ack == nil || ack.Stream == "" {
		return errors.New("JetStream publish returned an empty PubAck")
	}
	return nil
}

func validDestination(destinationType domain.DestinationType) bool {
	switch destinationType {
	case domain.DestinationHTTP, domain.DestinationKafka, domain.DestinationRabbit, domain.DestinationPostgres:
		return true
	default:
		return false
	}
}
