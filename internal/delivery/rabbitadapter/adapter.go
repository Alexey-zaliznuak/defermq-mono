package rabbitadapter

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/defermq/defermq/internal/delivery"
	"github.com/defermq/defermq/internal/domain"
	amqp "github.com/rabbitmq/amqp091-go"
)

type Config struct {
	URL              string
	AllowedExchanges []string
	ConnectTimeout   time.Duration
	PublishTimeout   time.Duration
	ReconnectInitial time.Duration
	ReconnectMax     time.Duration
	Mandatory        bool
}

// Adapter serializes access to the AMQP channel. amqp091 channels and their
// confirmation streams must not be shared by concurrent publishers without
// external synchronization.
type Adapter struct {
	mu      sync.Mutex
	config  Config
	allowed map[string]struct{}
	conn    *amqp.Connection
	channel *amqp.Channel
	returns <-chan amqp.Return
}

func New(config Config) (*Adapter, error) {
	if config.URL == "" || config.ConnectTimeout <= 0 || config.PublishTimeout <= 0 ||
		config.ReconnectInitial <= 0 || config.ReconnectMax < config.ReconnectInitial {
		return nil, errors.New("invalid RabbitMQ adapter configuration")
	}
	a := &Adapter{config: config, allowed: make(map[string]struct{}, len(config.AllowedExchanges))}
	for _, exchange := range config.AllowedExchanges {
		exchange = strings.TrimSpace(exchange)
		if exchange == "" {
			return nil, errors.New("RabbitMQ exchange allowlist contains an empty exchange")
		}
		a.allowed[exchange] = struct{}{}
	}
	return a, nil
}

func (a *Adapter) Type() domain.DestinationType { return domain.DestinationRabbit }

func (a *Adapter) Push(ctx context.Context, req delivery.PushRequest) error {
	target := req.Destination.Rabbit
	if target == nil {
		return delivery.NewPushError("invalid_destination", false, errors.New("RabbitMQ destination is missing"))
	}
	if len(a.allowed) > 0 {
		if _, ok := a.allowed[target.Exchange]; !ok {
			return delivery.NewPushError("exchange_not_allowed", false, fmt.Errorf("RabbitMQ exchange %q is not allowlisted", target.Exchange))
		}
	}
	operationCtx, cancel := context.WithTimeout(ctx, a.config.PublishTimeout)
	defer cancel()
	a.mu.Lock()
	defer a.mu.Unlock()
	if err := a.ensureConnected(operationCtx); err != nil {
		return delivery.NewPushError("rabbit_connect_failed", true, err)
	}

	headers := amqp.Table{}
	for name, value := range target.Headers {
		headers[name] = value
	}
	for name, value := range req.Headers {
		headers[name] = value
	}
	headers["idempotency-key"] = req.DeliveryID.String()
	headers["x-defermq-delivery-id"] = req.DeliveryID.String()
	headers["x-defermq-schedule-revision"] = strconv.FormatInt(req.ScheduleRevision, 10)
	headers["x-defermq-attempt"] = strconv.Itoa(req.Attempt)
	headers["x-defermq-scheduled-at"] = req.ScheduledAt.UTC().Format(time.RFC3339Nano)

	confirmation, err := a.channel.PublishWithDeferredConfirmWithContext(
		operationCtx,
		target.Exchange,
		target.RoutingKey,
		a.config.Mandatory,
		false,
		amqp.Publishing{
			DeliveryMode: amqp.Persistent,
			ContentType:  req.ContentType,
			Body:         req.Payload,
			Headers:      headers,
			MessageId:    req.DeliveryID.String(),
			Timestamp:    time.Now().UTC(),
		},
	)
	if err != nil {
		a.invalidate()
		return delivery.NewPushError("rabbit_publish_failed", rabbitRetryable(err), err)
	}
	acknowledged, err := confirmation.WaitContext(operationCtx)
	if err != nil {
		a.invalidate()
		return delivery.NewPushError("rabbit_confirm_failed", true, err)
	}
	if !acknowledged {
		return delivery.NewPushError("rabbit_nack", true, errors.New("broker negatively acknowledged publish"))
	}
	if a.config.Mandatory {
		select {
		case returned := <-a.returns:
			return delivery.NewPushError(
				"rabbit_unroutable",
				false,
				fmt.Errorf("message returned: code=%d text=%s", returned.ReplyCode, returned.ReplyText),
			)
		default:
		}
	}
	return nil
}

func (a *Adapter) Ready(ctx context.Context) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.ensureConnected(ctx)
}

func (a *Adapter) Close(context.Context) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	var first error
	if a.channel != nil {
		first = a.channel.Close()
		a.channel = nil
	}
	if a.conn != nil {
		if err := a.conn.Close(); err != nil && first == nil {
			first = err
		}
		a.conn = nil
	}
	return first
}

func (a *Adapter) ensureConnected(ctx context.Context) error {
	if a.conn != nil && !a.conn.IsClosed() && a.channel != nil && !a.channel.IsClosed() {
		return nil
	}
	a.invalidate()
	delay := a.config.ReconnectInitial
	var last error
	for {
		dialer := net.Dialer{Timeout: a.config.ConnectTimeout}
		conn, err := amqp.DialConfig(a.config.URL, amqp.Config{
			Dial: func(network, address string) (net.Conn, error) {
				return dialer.DialContext(ctx, network, address)
			},
		})
		if err == nil {
			channel, channelErr := conn.Channel()
			if channelErr == nil {
				channelErr = channel.Confirm(false)
			}
			if channelErr == nil {
				a.conn = conn
				a.channel = channel
				a.returns = channel.NotifyReturn(make(chan amqp.Return, 1))
				return nil
			}
			_ = conn.Close()
			err = channelErr
		}
		last = err
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return fmt.Errorf("RabbitMQ reconnect: %w: %v", ctx.Err(), last)
		case <-timer.C:
		}
		delay *= 2
		if delay > a.config.ReconnectMax {
			delay = a.config.ReconnectMax
		}
	}
}

func (a *Adapter) invalidate() {
	if a.channel != nil {
		_ = a.channel.Close()
	}
	if a.conn != nil {
		_ = a.conn.Close()
	}
	a.channel = nil
	a.conn = nil
	a.returns = nil
}

func rabbitRetryable(err error) bool {
	var amqpError *amqp.Error
	if errors.As(err, &amqpError) {
		switch amqpError.Code {
		case 403, 404, 405, 406:
			return false
		}
	}
	return true
}
