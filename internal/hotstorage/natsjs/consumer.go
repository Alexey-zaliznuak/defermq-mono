package natsjs

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/defermq/defermq/internal/domain"
	"github.com/nats-io/nats.go/jetstream"
)

type DurableConsumerConfig struct {
	Stream        string
	Durable       string
	Destination   domain.DestinationType
	AckWait       time.Duration
	MaxAckPending int
	MaxDeliver    int
	MaxFetchBatch int
	MaxFetchWait  time.Duration
}

func (c DurableConsumerConfig) Validate() error {
	if c.Stream == "" || c.Durable == "" {
		return errors.New("stream and durable consumer names are required")
	}
	if !validDestination(c.Destination) {
		return fmt.Errorf("invalid destination type %q", c.Destination)
	}
	if c.AckWait <= 0 || c.MaxAckPending <= 0 || c.MaxDeliver <= 0 {
		return errors.New("AckWait, MaxAckPending and MaxDeliver must be positive")
	}
	return nil
}

func EnsureDurablePull(
	ctx context.Context,
	js jetstream.JetStream,
	subjects Subjects,
	cfg DurableConsumerConfig,
) (jetstream.Consumer, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	consumer, err := js.CreateOrUpdateConsumer(ctx, cfg.Stream, jetstream.ConsumerConfig{
		Name:              cfg.Durable,
		Durable:           cfg.Durable,
		DeliverPolicy:     jetstream.DeliverAllPolicy,
		AckPolicy:         jetstream.AckExplicitPolicy,
		AckWait:           cfg.AckWait,
		MaxDeliver:        cfg.MaxDeliver,
		FilterSubject:     subjects.Ready(cfg.Destination),
		ReplayPolicy:      jetstream.ReplayInstantPolicy,
		MaxAckPending:     cfg.MaxAckPending,
		MaxRequestBatch:   cfg.MaxFetchBatch,
		MaxRequestExpires: cfg.MaxFetchWait,
	})
	if err != nil {
		return nil, fmt.Errorf("ensure durable consumer %q: %w", cfg.Durable, err)
	}
	return consumer, nil
}

func FetchBatch(ctx context.Context, consumer jetstream.Consumer, batch int, maxWait time.Duration) (jetstream.MessageBatch, error) {
	if batch <= 0 || maxWait <= 0 {
		return nil, errors.New("fetch batch and max wait must be positive")
	}
	result, err := consumer.Fetch(batch, jetstream.FetchMaxWait(maxWait))
	if err != nil {
		return nil, fmt.Errorf("fetch JetStream messages: %w", err)
	}
	return result, nil
}

func DecodeReadyEvent(data []byte, expected domain.DestinationType) (ReadyEvent, error) {
	var event ReadyEvent
	if err := json.Unmarshal(data, &event); err != nil {
		return ReadyEvent{}, fmt.Errorf("decode ready event: %w", err)
	}
	if err := event.Validate(expected); err != nil {
		return ReadyEvent{}, err
	}
	event.DeliverAt = event.DeliverAt.UTC()
	return event, nil
}
