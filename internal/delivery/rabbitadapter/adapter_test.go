package rabbitadapter

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/defermq/defermq/internal/delivery"
	"github.com/defermq/defermq/internal/domain"
)

func TestExchangeAllowlistRejectsBeforeConnection(t *testing.T) {
	adapter, err := New(Config{
		URL:              "amqp://guest:guest@127.0.0.1:1/",
		AllowedExchanges: []string{"allowed"},
		ConnectTimeout:   time.Millisecond,
		PublishTimeout:   time.Millisecond,
		ReconnectInitial: time.Millisecond,
		ReconnectMax:     time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	err = adapter.Push(context.Background(), delivery.PushRequest{
		Destination: domain.Destination{
			Type:   domain.DestinationRabbit,
			Rabbit: &domain.RabbitDestination{Exchange: "blocked"},
		},
	})
	var pushErr *delivery.PushError
	if !errors.As(err, &pushErr) || pushErr.Retryable || pushErr.Code != "exchange_not_allowed" {
		t.Fatalf("Push() error = %#v", err)
	}
}
