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
	OutboxHotRegister OutboxKind = "hot_register"
	OutboxReady       OutboxKind = "ready"
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

type AsyncAckPublisher interface {
	PublishMsgAsync(*nats.Msg, ...jetstream.PublishOpt) (jetstream.PubAckFuture, error)
}

type Publisher struct {
	js       AckPublisher
	subjects Subjects
	now      func() time.Time
}

func NewPublisher(js AckPublisher, subjects Subjects) *Publisher {
	return &Publisher{js: js, subjects: subjects, now: time.Now}
}

func (p *Publisher) Decide(req PublishRequest, _ time.Time) (PublishDecision, error) {
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
	default:
		return PublishDecision{}, fmt.Errorf("invalid outbox kind %q", req.Kind)
	}
}

// PublishReadyBatch starts all publishes before awaiting their PubAcks. It
// returns the requests durably acknowledged by JetStream even when peers fail.
func (p *Publisher) PublishReadyBatch(
	ctx context.Context,
	requests []PublishRequest,
) ([]PublishRequest, error) {
	async, ok := p.js.(AsyncAckPublisher)
	if !ok {
		return nil, errors.New("JetStream publisher does not support async PubAck")
	}
	type pending struct {
		request PublishRequest
		future  jetstream.PubAckFuture
	}
	pendingAcks := make([]pending, 0, len(requests))
	var failures []error
	for _, request := range requests {
		if request.Kind != OutboxReady {
			failures = append(failures, fmt.Errorf("batch contains non-ready kind %q", request.Kind))
			continue
		}
		msg, err := p.message(request)
		if err != nil {
			failures = append(failures, err)
			continue
		}
		future, err := async.PublishMsgAsync(msg, jetstream.WithMsgID(
			MessageID(request.DeliveryID, request.ScheduleRevision, string(OutboxReady)),
		))
		if err != nil {
			failures = append(failures, err)
			continue
		}
		pendingAcks = append(pendingAcks, pending{request: request, future: future})
	}
	published := make([]PublishRequest, 0, len(pendingAcks))
	for _, item := range pendingAcks {
		select {
		case ack := <-item.future.Ok():
			if ack == nil || ack.Stream == "" {
				failures = append(failures, errors.New("JetStream async publish returned an empty PubAck"))
			} else {
				published = append(published, item.request)
			}
		case err := <-item.future.Err():
			failures = append(failures, err)
		case <-ctx.Done():
			failures = append(failures, ctx.Err())
		}
	}
	return published, errors.Join(failures...)
}

func (p *Publisher) message(req PublishRequest) (*nats.Msg, error) {
	decision, err := p.Decide(req, p.now().UTC())
	if err != nil {
		return nil, err
	}
	event := ReadyEvent{
		SchemaVersion: EventSchemaVersion, DeliveryID: req.DeliveryID,
		ScheduleRevision: req.ScheduleRevision, DeliverAt: req.DeliverAt.UTC(),
		DestinationType: req.DestinationType,
	}
	body, err := json.Marshal(event)
	if err != nil {
		return nil, fmt.Errorf("encode ready event: %w", err)
	}
	msg := nats.NewMsg(decision.Subject)
	msg.Data = body
	return msg, nil
}

func (p *Publisher) Publish(ctx context.Context, req PublishRequest) error {
	decision, err := p.Decide(req, p.now().UTC())
	if err != nil {
		return err
	}
	msg, err := p.message(req)
	if err != nil {
		return err
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
