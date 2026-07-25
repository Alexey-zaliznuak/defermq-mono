package postgres

import (
	"errors"
	"fmt"
	"time"

	"github.com/defermq/defermq/internal/domain"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

type OutboxKind string

const (
	OutboxHotRegister OutboxKind = "hot_register"
	OutboxReady       OutboxKind = "ready"
)

type OutboxItem struct {
	ID               int64
	DeliveryID       uuid.UUID
	ScheduleRevision int64
	Kind             OutboxKind
	DeliverAt        *time.Time
	DestinationType  domain.DestinationType
	AvailableAt      time.Time
	LockedBy         *string
	LockedUntil      *time.Time
	PublishAttempts  int
	PublishedAt      *time.Time
	LastError        *string
	CreatedAt        time.Time
}

type CreateDeliveryParams struct {
	Delivery   domain.Delivery
	Payload    domain.Payload
	HotHorizon time.Duration
}

type RescheduleParams struct {
	DeliveryID uuid.UUID
	DeliverAt  time.Time
	HotHorizon time.Duration
}

type ClaimDeliveryParams struct {
	DeliveryID         uuid.UUID
	ScheduleRevision   int64
	Owner              string
	Lease              time.Duration
	ClockSkewTolerance time.Duration
}

type ClaimRequest struct {
	DeliveryID       uuid.UUID
	ScheduleRevision int64
}

type BatchClaimResult struct {
	Delivery *domain.Delivery
	Payload  *domain.Payload
	Reason   string
	Wait     time.Duration
}

type RetryDeliveryParams struct {
	DeliveryID uuid.UUID
	Owner      string
	Delay      time.Duration
	HotHorizon time.Duration
	LastError  string
}

type RetentionResult struct {
	Deliveries int64
	Payloads   int64
	Outbox     int64
}

type ClaimRejection struct {
	Exists           bool
	Status           domain.DeliveryStatus
	ScheduleRevision int64
	DeliverAt        time.Time
}

func (r ClaimRejection) Error() string {
	if !r.Exists {
		return "delivery not found"
	}
	return fmt.Sprintf("delivery cannot be claimed: status=%s revision=%d", r.Status, r.ScheduleRevision)
}

type rowScanner interface {
	Scan(dest ...any) error
}

func scanDelivery(row rowScanner) (domain.Delivery, error) {
	var d domain.Delivery
	err := row.Scan(
		&d.ID, &d.PayloadID, &d.IdempotencyKey, &d.DestinationType, &d.Destination,
		&d.DeliverAt, &d.Status, &d.ScheduleRevision, &d.HotRegisteredRevision,
		&d.Attempts, &d.MaxAttempts, &d.ProcessingOwner, &d.ProcessingUntil,
		&d.LastError, &d.LastAttemptAt, &d.DeliveredAt, &d.CancelledAt,
		&d.CreatedAt, &d.UpdatedAt,
	)
	if err != nil {
		return domain.Delivery{}, mapError(err)
	}
	d.DeliverAt = d.DeliverAt.UTC()
	d.CreatedAt = d.CreatedAt.UTC()
	d.UpdatedAt = d.UpdatedAt.UTC()
	return d, nil
}

func scanOutbox(row rowScanner) (OutboxItem, error) {
	var item OutboxItem
	err := row.Scan(
		&item.ID, &item.DeliveryID, &item.ScheduleRevision, &item.Kind,
		&item.DeliverAt, &item.DestinationType, &item.AvailableAt,
		&item.LockedBy, &item.LockedUntil, &item.PublishAttempts,
		&item.PublishedAt, &item.LastError, &item.CreatedAt,
	)
	if err != nil {
		return OutboxItem{}, mapError(err)
	}
	return item, nil
}

func mapError(err error) error {
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.ErrNotFound
	}
	return err
}

func isUniqueViolation(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "23505"
}

const deliveryColumns = `
	id, payload_id, idempotency_key, destination_type, destination,
	deliver_at, status, schedule_revision, hot_registered_revision,
	attempts, max_attempts, processing_owner, processing_until,
	last_error, last_attempt_at, delivered_at, cancelled_at,
	created_at, updated_at`

const outboxColumns = `
	o.id, o.delivery_id, o.schedule_revision, o.kind, o.deliver_at, o.destination_type,
	o.available_at, o.locked_by, o.locked_until, o.publish_attempts, o.published_at,
	o.last_error, o.created_at`
