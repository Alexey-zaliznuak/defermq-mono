package natsjs

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/defermq/defermq/internal/domain"
	"github.com/google/uuid"
)

func TestScheduledDeliveryIntegration(t *testing.T) {
	url := os.Getenv("DEFERMQ_NATS_INTEGRATION_URL")
	if url == "" {
		t.Skip("set DEFERMQ_NATS_INTEGRATION_URL to run NATS 2.12+ integration tests")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	connection, err := Connect(ctx, ConnectionConfig{
		URL: url, Name: "defermq-integration-test", ConnectTimeout: 3 * time.Second,
		ReconnectWait: time.Second, MaxReconnects: 0,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = connection.Close(context.Background()) }()

	suffix := uuid.NewString()
	streamConfig := StreamConfig{
		Name: "T" + suffix[:8],
		Subjects: Subjects{
			SchedulePrefix: "test." + suffix + ".schedule",
			ReadyPrefix:    "test." + suffix + ".ready",
		},
		Replicas: 1, MaxAge: time.Minute, MaxBytes: 1 << 20,
		MaxMsgSize: 64 << 10, DuplicateWindow: time.Minute,
	}
	if _, err := EnsureStream(ctx, connection.JS, streamConfig); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = connection.JS.DeleteStream(context.Background(), streamConfig.Name) }()
	consumer, err := EnsureDurablePull(ctx, connection.JS, streamConfig.Subjects, DurableConsumerConfig{
		Stream: streamConfig.Name, Durable: "http", Destination: domain.DestinationHTTP,
		AckWait: time.Minute, MaxAckPending: 10, MaxDeliver: 3, MaxFetchBatch: 1, MaxFetchWait: 5 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	deliverAt := time.Now().UTC()
	publisher := NewPublisher(connection.JS, streamConfig.Subjects)
	if err := publisher.Publish(ctx, PublishRequest{
		Kind: OutboxReady, DeliveryID: uuid.New(), ScheduleRevision: 1,
		DeliverAt: deliverAt, DestinationType: domain.DestinationHTTP,
	}); err != nil {
		t.Fatal(err)
	}
	batch, err := FetchBatch(ctx, consumer, 1, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	message, ok := <-batch.Messages()
	if !ok {
		t.Fatalf("scheduled message not received: %v", batch.Error())
	}
	event, err := DecodeReadyEvent(message.Data(), domain.DestinationHTTP)
	if err != nil {
		t.Fatal(err)
	}
	if !event.DeliverAt.Equal(deliverAt) {
		t.Fatalf("deliver_at = %s, want %s", event.DeliverAt, deliverAt)
	}
	if err := message.Ack(); err != nil {
		t.Fatal(err)
	}
}
