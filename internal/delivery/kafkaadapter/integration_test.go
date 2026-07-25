package kafkaadapter

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/defermq/defermq/internal/delivery"
	"github.com/defermq/defermq/internal/domain"
	"github.com/google/uuid"
)

func TestIntegrationSyncPublish(t *testing.T) {
	brokers := strings.TrimSpace(os.Getenv("DEFERMQ_TEST_KAFKA_BROKERS"))
	topic := strings.TrimSpace(os.Getenv("DEFERMQ_TEST_KAFKA_TOPIC"))
	if brokers == "" || topic == "" {
		t.Skip("set DEFERMQ_TEST_KAFKA_BROKERS and DEFERMQ_TEST_KAFKA_TOPIC to run Kafka integration test")
	}
	adapter, err := New(Config{
		Brokers:        strings.Split(brokers, ","),
		ClientID:       "defermq-integration-test",
		AllowedTopics:  []string{topic},
		DialTimeout:    5 * time.Second,
		RequestTimeout: 15 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer adapter.Close(context.Background())
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := adapter.Ready(ctx); err != nil {
		t.Fatal(err)
	}
	if err := adapter.Push(ctx, delivery.PushRequest{
		DeliveryID:       uuid.New(),
		ScheduleRevision: 1,
		Attempt:          1,
		ScheduledAt:      time.Now().UTC(),
		Destination: domain.Destination{
			Type:  domain.DestinationKafka,
			Kafka: &domain.KafkaDestination{Topic: topic},
		},
		Payload: []byte("defermq integration test"),
	}); err != nil {
		t.Fatal(err)
	}
}
