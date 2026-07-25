package pusher

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/defermq/defermq/internal/domain"
	"github.com/defermq/defermq/internal/hotstorage/natsjs"
	"github.com/nats-io/nats.go/jetstream"
)

type NATSConsumerConfig struct {
	Stream        string
	Durable       string
	Subjects      natsjs.Subjects
	Type          domain.DestinationType
	AckWait       time.Duration
	MaxAckPending int
	MaxDeliver    int
	MaxBatch      int
	MaxWait       time.Duration
}

type NATSConsumer struct {
	consumer jetstream.Consumer
	messages jetstream.MessagesContext
	typ      domain.DestinationType
}

func NewNATSConsumer(
	ctx context.Context,
	js jetstream.JetStream,
	config NATSConsumerConfig,
) (*NATSConsumer, error) {
	if js == nil || config.Stream == "" || config.Durable == "" ||
		config.Subjects.ReadyPrefix == "" || config.Type == "" || config.AckWait <= 0 ||
		config.MaxAckPending <= 0 || config.MaxDeliver <= 0 ||
		config.MaxBatch <= 0 || config.MaxWait <= 0 {
		return nil, errors.New("invalid NATS consumer configuration")
	}
	consumer, err := natsjs.EnsureDurablePull(ctx, js, config.Subjects, natsjs.DurableConsumerConfig{
		Stream:        config.Stream,
		Durable:       config.Durable,
		Destination:   config.Type,
		AckWait:       config.AckWait,
		MaxAckPending: config.MaxAckPending,
		MaxDeliver:    config.MaxDeliver,
		MaxFetchBatch: config.MaxBatch,
		MaxFetchWait:  config.MaxWait,
	})
	if err != nil {
		return nil, fmt.Errorf("ensure NATS consumer %q: %w", config.Durable, err)
	}
	messages, err := consumer.Messages(
		jetstream.PullMaxMessages(config.MaxBatch),
		jetstream.PullExpiry(config.MaxWait),
	)
	if err != nil {
		return nil, fmt.Errorf("create NATS message iterator %q: %w", config.Durable, err)
	}
	return &NATSConsumer{consumer: consumer, messages: messages, typ: config.Type}, nil
}

func (c *NATSConsumer) Type() domain.DestinationType { return c.typ }

func (c *NATSConsumer) Next(ctx context.Context) (Message, error) {
	message, err := c.messages.Next(jetstream.NextContext(ctx))
	if err != nil {
		return nil, err
	}
	return natsMessage{message: message}, nil
}

func (c *NATSConsumer) Ready(ctx context.Context) error {
	_, err := c.consumer.Info(ctx)
	return err
}

func (c *NATSConsumer) Pending(ctx context.Context) (uint64, int, error) {
	info, err := c.consumer.Info(ctx)
	if err != nil {
		return 0, 0, err
	}
	return info.NumPending, info.NumAckPending, nil
}

func (c *NATSConsumer) Close(context.Context) error {
	c.messages.Stop()
	return nil
}

type natsMessage struct {
	message jetstream.Msg
}

func (m natsMessage) Data() []byte { return m.message.Data() }

func (m natsMessage) Ack(ctx context.Context) error {
	return m.message.DoubleAck(ctx)
}

func (m natsMessage) Nak(_ context.Context, delay time.Duration) error {
	if delay <= 0 {
		return m.message.Nak()
	}
	return m.message.NakWithDelay(delay)
}

func (m natsMessage) Term(context.Context) error {
	return m.message.Term()
}
