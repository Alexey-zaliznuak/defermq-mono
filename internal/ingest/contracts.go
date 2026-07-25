package ingest

import (
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"strings"
	"time"

	"github.com/defermq/defermq/internal/domain"
	"github.com/google/uuid"
)

const (
	SchemaVersion     = 1
	DefaultShardCount = 32
)

type Kind string

const (
	KindCreate     Kind = "create"
	KindCancel     Kind = "cancel"
	KindReschedule Kind = "reschedule"
)

var (
	deliveryNamespace = uuid.MustParse("27cc7bb8-13df-5d8a-a32f-fbbce1f26f65")
	payloadNamespace  = uuid.MustParse("643be16d-0564-59e0-b5e1-b0b39cc51481")
)

type Command struct {
	SchemaVersion    int                    `json:"schema_version"`
	Kind             Kind                   `json:"kind"`
	CommandID        uuid.UUID              `json:"command_id"`
	DeliveryID       uuid.UUID              `json:"delivery_id"`
	PayloadID        uuid.UUID              `json:"payload_id,omitempty"`
	IdempotencyKey   *string                `json:"idempotency_key,omitempty"`
	Destination      json.RawMessage        `json:"destination,omitempty"`
	DestinationType  domain.DestinationType `json:"destination_type,omitempty"`
	DeliverAt        time.Time              `json:"deliver_at,omitempty"`
	MaxAttempts      int                    `json:"max_attempts,omitempty"`
	Payload          *Payload               `json:"payload,omitempty"`
	HotHorizon       time.Duration          `json:"hot_horizon,omitempty"`
	ExpectedRevision int64                  `json:"expected_revision,omitempty"`
}

type Payload struct {
	Body        []byte            `json:"body"`
	Headers     map[string]string `json:"headers"`
	ContentType string            `json:"content_type"`
	SizeBytes   int64             `json:"size_bytes"`
}

func IDs(idempotencyKey *string) (uuid.UUID, uuid.UUID, error) {
	if idempotencyKey != nil {
		key := strings.TrimSpace(*idempotencyKey)
		if key == "" {
			return uuid.Nil, uuid.Nil, errors.New("idempotency key is empty")
		}
		return uuid.NewSHA1(deliveryNamespace, []byte(key)), uuid.NewSHA1(payloadNamespace, []byte(key)), nil
	}
	deliveryID, err := uuid.NewV7()
	if err != nil {
		return uuid.Nil, uuid.Nil, fmt.Errorf("generate delivery ID: %w", err)
	}
	payloadID, err := uuid.NewV7()
	if err != nil {
		return uuid.Nil, uuid.Nil, fmt.Errorf("generate payload ID: %w", err)
	}
	return deliveryID, payloadID, nil
}

func (c Command) Validate() error {
	if c.SchemaVersion != SchemaVersion || c.CommandID == uuid.Nil || c.DeliveryID == uuid.Nil {
		return errors.New("invalid ingest command envelope")
	}
	switch c.Kind {
	case KindCreate:
		if c.PayloadID == uuid.Nil || c.Payload == nil || c.DeliverAt.IsZero() ||
			c.MaxAttempts <= 0 || c.Payload.ContentType == "" ||
			c.Payload.SizeBytes != int64(len(c.Payload.Body)) ||
			len(c.Destination) == 0 || c.HotHorizon < 0 {
			return errors.New("invalid create command")
		}
		switch c.DestinationType {
		case domain.DestinationHTTP, domain.DestinationKafka, domain.DestinationRabbit, domain.DestinationPostgres:
		default:
			return errors.New("invalid create destination type")
		}
	case KindCancel:
		if c.ExpectedRevision <= 0 {
			return errors.New("invalid cancel command")
		}
	case KindReschedule:
		if c.DeliverAt.IsZero() || c.HotHorizon < 0 || c.ExpectedRevision <= 0 {
			return errors.New("invalid reschedule command")
		}
	default:
		return fmt.Errorf("unknown ingest command kind %q", c.Kind)
	}
	return nil
}

func MessageID(c Command) string {
	return c.CommandID.String()
}

func Shard(deliveryID uuid.UUID, shardCount int) int {
	if shardCount <= 0 {
		shardCount = DefaultShardCount
	}
	hash := fnv.New32a()
	_, _ = hash.Write(deliveryID[:])
	return int(hash.Sum32() % uint32(shardCount))
}

func ShardSubject(prefix string, deliveryID uuid.UUID, shardCount int) string {
	return fmt.Sprintf("%s.%d", strings.TrimSuffix(prefix, ".*"), Shard(deliveryID, shardCount))
}

func StreamSubject(prefix string) string {
	return strings.TrimSuffix(prefix, ".*") + ".*"
}

func DLQSubject(prefix string) string {
	return strings.TrimSuffix(prefix, ".*") + "-dlq"
}
