package rabbitadapter

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/defermq/defermq/internal/delivery"
	"github.com/defermq/defermq/internal/domain"
	"github.com/google/uuid"
)

func TestIntegrationConfirmedPublish(t *testing.T) {
	url := os.Getenv("DEFERMQ_TEST_RABBIT_URL")
	routingKey := os.Getenv("DEFERMQ_TEST_RABBIT_ROUTING_KEY")
	if url == "" || routingKey == "" {
		t.Skip("set DEFERMQ_TEST_RABBIT_URL and DEFERMQ_TEST_RABBIT_ROUTING_KEY to run RabbitMQ integration test")
	}
	adapter, err := New(Config{
		URL:              url,
		ConnectTimeout:   5 * time.Second,
		PublishTimeout:   15 * time.Second,
		ReconnectInitial: 100 * time.Millisecond,
		ReconnectMax:     time.Second,
		Mandatory:        true,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer adapter.Close(context.Background())
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := adapter.Push(ctx, delivery.PushRequest{
		DeliveryID:       uuid.New(),
		ScheduleRevision: 1,
		Attempt:          1,
		ScheduledAt:      time.Now().UTC(),
		Destination: domain.Destination{
			Type: domain.DestinationRabbit,
			Rabbit: &domain.RabbitDestination{
				Exchange:   os.Getenv("DEFERMQ_TEST_RABBIT_EXCHANGE"),
				RoutingKey: routingKey,
			},
		},
		Payload:     []byte("defermq integration test"),
		ContentType: "text/plain",
	}); err != nil {
		t.Fatal(err)
	}
}
