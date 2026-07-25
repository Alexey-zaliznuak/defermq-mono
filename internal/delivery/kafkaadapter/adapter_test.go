package kafkaadapter

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/defermq/defermq/internal/delivery"
	"github.com/defermq/defermq/internal/domain"
	"github.com/google/uuid"
)

func TestTopicAllowlistRejectsWithoutPublishing(t *testing.T) {
	adapter, err := New(Config{
		Brokers:        []string{"127.0.0.1:1"},
		ClientID:       "test",
		AllowedTopics:  []string{"allowed"},
		DialTimeout:    time.Millisecond,
		RequestTimeout: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer adapter.Close(context.Background())
	err = adapter.Push(context.Background(), delivery.PushRequest{
		DeliveryID: uuid.New(),
		Destination: domain.Destination{
			Type:  domain.DestinationKafka,
			Kafka: &domain.KafkaDestination{Topic: "blocked"},
		},
	})
	var pushErr *delivery.PushError
	if !errors.As(err, &pushErr) || pushErr.Retryable || pushErr.Code != "topic_not_allowed" {
		t.Fatalf("Push() error = %#v", err)
	}
}
