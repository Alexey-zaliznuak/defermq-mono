package domain

import (
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
)

type DestinationType string

const (
	DestinationHTTP     DestinationType = "http"
	DestinationKafka    DestinationType = "kafka"
	DestinationRabbit   DestinationType = "rabbit"
	DestinationPostgres DestinationType = "postgres"
)

var ErrInvalidDestination = errors.New("invalid destination")

type Destination struct {
	Type     DestinationType      `json:"type"`
	HTTP     *HTTPDestination     `json:"http,omitempty"`
	Kafka    *KafkaDestination    `json:"kafka,omitempty"`
	Rabbit   *RabbitDestination   `json:"rabbit,omitempty"`
	Postgres *PostgresDestination `json:"postgres,omitempty"`
}

type HTTPDestination struct {
	URL     string            `json:"url"`
	Method  string            `json:"method,omitempty"`
	Headers map[string]string `json:"headers,omitempty"`
}

type KafkaDestination struct {
	Topic   string            `json:"topic"`
	Key     string            `json:"key,omitempty"`
	Headers map[string]string `json:"headers,omitempty"`
}

type RabbitDestination struct {
	Exchange   string            `json:"exchange"`
	RoutingKey string            `json:"routing_key"`
	Headers    map[string]string `json:"headers,omitempty"`
}

type PostgresDestination struct {
	Channel  string            `json:"channel,omitempty"`
	Metadata map[string]string `json:"metadata,omitempty"`
}

var reservedHeaders = map[string]struct{}{
	"idempotency-key":             {},
	"x-defermq-delivery-id":       {},
	"x-defermq-schedule-revision": {},
	"x-defermq-attempt":           {},
	"x-defermq-scheduled-at":      {},
}

func (d *Destination) Validate(enabled map[DestinationType]bool) error {
	if d == nil || !enabled[d.Type] {
		return fmt.Errorf("%w: destination type %q is disabled or unknown", ErrInvalidDestination, d.Type)
	}
	sections := 0
	for _, present := range []bool{d.HTTP != nil, d.Kafka != nil, d.Rabbit != nil, d.Postgres != nil} {
		if present {
			sections++
		}
	}
	if sections != 1 {
		return fmt.Errorf("%w: exactly one destination section is required", ErrInvalidDestination)
	}
	switch d.Type {
	case DestinationHTTP:
		if d.HTTP == nil {
			return fmt.Errorf("%w: http section is required", ErrInvalidDestination)
		}
		if d.HTTP.Method == "" {
			d.HTTP.Method = http.MethodPost
		}
		d.HTTP.Method = strings.ToUpper(d.HTTP.Method)
		if d.HTTP.Method != http.MethodPost && d.HTTP.Method != http.MethodPut && d.HTTP.Method != http.MethodPatch {
			return fmt.Errorf("%w: unsupported HTTP method", ErrInvalidDestination)
		}
		u, err := url.ParseRequestURI(d.HTTP.URL)
		if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" || u.User != nil {
			return fmt.Errorf("%w: invalid HTTP URL", ErrInvalidDestination)
		}
		if err := validateHeaders(d.HTTP.Headers); err != nil {
			return err
		}
	case DestinationKafka:
		if d.Kafka == nil || strings.TrimSpace(d.Kafka.Topic) == "" {
			return fmt.Errorf("%w: kafka topic is required", ErrInvalidDestination)
		}
		if err := validateHeaders(d.Kafka.Headers); err != nil {
			return err
		}
	case DestinationRabbit:
		if d.Rabbit == nil {
			return fmt.Errorf("%w: rabbit section is required", ErrInvalidDestination)
		}
		if err := validateHeaders(d.Rabbit.Headers); err != nil {
			return err
		}
	case DestinationPostgres:
		if d.Postgres == nil {
			return fmt.Errorf("%w: postgres section is required", ErrInvalidDestination)
		}
	default:
		return fmt.Errorf("%w: unknown type", ErrInvalidDestination)
	}
	return nil
}

func validateHeaders(headers map[string]string) error {
	if len(headers) > 64 {
		return fmt.Errorf("%w: too many headers", ErrInvalidDestination)
	}
	for key, value := range headers {
		if !validHeaderName(key) || len(key) > 256 || len(value) > 8192 {
			return fmt.Errorf("%w: invalid header", ErrInvalidDestination)
		}
		if _, reserved := reservedHeaders[strings.ToLower(key)]; reserved {
			return fmt.Errorf("%w: reserved header %q", ErrInvalidDestination, key)
		}
	}
	return nil
}

func ValidateHeaders(headers map[string]string) error {
	return validateHeaders(headers)
}

func validHeaderName(name string) bool {
	if name == "" {
		return false
	}
	for _, r := range name {
		if !(r >= 'a' && r <= 'z') && !(r >= 'A' && r <= 'Z') && !(r >= '0' && r <= '9') &&
			!strings.ContainsRune("!#$%&'*+-.^_`|~", r) {
			return false
		}
	}
	return true
}
