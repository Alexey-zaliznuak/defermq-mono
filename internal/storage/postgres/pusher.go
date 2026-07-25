package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/defermq/defermq/internal/domain"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// ClaimDeliveriesBatch locks all requested deliveries in UUID order and
// atomically claims every eligible delivery. Results retain input order.
func (s *Store) ClaimDeliveriesBatch(
	ctx context.Context,
	requests []ClaimRequest,
	owner string,
	lease time.Duration,
	tolerance time.Duration,
) ([]BatchClaimResult, error) {
	if len(requests) == 0 {
		return []BatchClaimResult{}, nil
	}
	if owner == "" || lease <= 0 || tolerance < 0 {
		return nil, errors.New("invalid batch delivery claim parameters")
	}
	ids := make([]uuid.UUID, len(requests))
	revisions := make([]int64, len(requests))
	for index, request := range requests {
		if request.DeliveryID == uuid.Nil || request.ScheduleRevision <= 0 {
			return nil, errors.New("invalid batch delivery claim request")
		}
		ids[index] = request.DeliveryID
		revisions[index] = request.ScheduleRevision
	}

	ctx, cancel := s.withTimeout(ctx)
	defer cancel()
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return nil, fmt.Errorf("begin batch delivery claim: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	rows, err := tx.Query(ctx, `
		WITH requested AS MATERIALIZED (
			SELECT input.id, input.revision, input.ordinality::bigint AS ord
			FROM unnest($1::uuid[], $2::bigint[]) WITH ORDINALITY
				AS input(id, revision, ordinality)
		), requested_ids AS MATERIALIZED (
			SELECT id
			FROM requested
			GROUP BY id
			ORDER BY id
		), db_time AS MATERIALIZED (
			SELECT clock_timestamp() AS now
		), locked_deliveries AS MATERIALIZED (
			SELECT d.*
			FROM deliveries d
			JOIN requested_ids requested_id ON requested_id.id = d.id
			ORDER BY d.id
			FOR UPDATE OF d
		), eligible AS MATERIALIZED (
			SELECT DISTINCT ON (d.id) d.id, r.ord
			FROM locked_deliveries d
			JOIN requested r
			  ON r.id = d.id
			 AND r.revision = d.schedule_revision
			CROSS JOIN db_time t
			WHERE d.status = 'scheduled'
			  AND d.deliver_at <= t.now + $5 * interval '1 millisecond'
			ORDER BY d.id, r.ord
		), updated AS MATERIALIZED (
			UPDATE deliveries d
			SET status = 'processing',
				processing_owner = $3,
				processing_until = t.now + $4 * interval '1 millisecond',
				attempts = d.attempts + 1,
				last_attempt_at = t.now,
				updated_at = t.now
			FROM eligible e
			CROSS JOIN db_time t
			WHERE d.id = e.id
			RETURNING d.*, e.ord
		)
		SELECT r.ord,
			CASE
				WHEN u.id IS NOT NULL THEN 'claimed'
				WHEN e.id IS NOT NULL THEN 'processing'
				WHEN d.id IS NULL THEN 'not_found'
				WHEN d.schedule_revision <> r.revision THEN 'stale_revision'
				WHEN d.status IN ('delivered', 'cancelled', 'dead') THEN 'terminal'
				WHEN d.status = 'processing' THEN 'processing'
				WHEN d.deliver_at > t.now + $5 * interval '1 millisecond' THEN 'too_early'
				ELSE 'stale_revision'
			END AS reason,
			CASE WHEN d.deliver_at IS NULL THEN 0
				ELSE GREATEST(0, EXTRACT(EPOCH FROM (d.deliver_at - t.now)) * 1000)
			END AS wait_ms,
			CASE WHEN u.id IS NULL THEN NULL ELSE to_jsonb(u) - 'ord' END AS delivery,
			p.id, p.body, p.headers, p.content_type, p.size_bytes, p.created_at
		FROM requested r
		CROSS JOIN db_time t
		LEFT JOIN locked_deliveries d ON d.id = r.id
		LEFT JOIN eligible e ON e.id = r.id
		LEFT JOIN updated u ON u.ord = r.ord
		LEFT JOIN message_payloads p ON p.id = u.payload_id
		ORDER BY r.ord`,
		ids, revisions, owner, durationMillis(lease), durationMillis(tolerance))
	if err != nil {
		return nil, fmt.Errorf("batch claim deliveries: %w", err)
	}
	defer rows.Close()

	results := make([]BatchClaimResult, 0, len(requests))
	for rows.Next() {
		var (
			ordinal     int64
			reason      string
			waitMillis  float64
			deliveryRaw []byte
			payloadID   *uuid.UUID
			body        []byte
			headersRaw  []byte
			contentType *string
			sizeBytes   *int64
			createdAt   *time.Time
		)
		if err := rows.Scan(
			&ordinal, &reason, &waitMillis, &deliveryRaw,
			&payloadID, &body, &headersRaw, &contentType, &sizeBytes, &createdAt,
		); err != nil {
			return nil, fmt.Errorf("scan batch delivery claim: %w", err)
		}
		result, err := decodeBatchClaimRow(
			reason, time.Duration(waitMillis*float64(time.Millisecond)), deliveryRaw,
			payloadID, body, headersRaw, contentType, sizeBytes, createdAt,
		)
		if err != nil {
			return nil, fmt.Errorf("decode batch delivery claim at ordinal %d: %w", ordinal, err)
		}
		results = append(results, result)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read batch delivery claims: %w", err)
	}
	if len(results) != len(requests) {
		return nil, fmt.Errorf("batch delivery claim returned %d results for %d requests", len(results), len(requests))
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit batch delivery claim: %w", err)
	}
	return results, nil
}

type batchDeliveryJSON struct {
	ID                    uuid.UUID              `json:"id"`
	PayloadID             uuid.UUID              `json:"payload_id"`
	IdempotencyKey        *string                `json:"idempotency_key"`
	DestinationType       domain.DestinationType `json:"destination_type"`
	Destination           json.RawMessage        `json:"destination"`
	DeliverAt             time.Time              `json:"deliver_at"`
	Status                domain.DeliveryStatus  `json:"status"`
	ScheduleRevision      int64                  `json:"schedule_revision"`
	HotRegisteredRevision *int64                 `json:"hot_registered_revision"`
	Attempts              int                    `json:"attempts"`
	MaxAttempts           int                    `json:"max_attempts"`
	ProcessingOwner       *string                `json:"processing_owner"`
	ProcessingUntil       *time.Time             `json:"processing_until"`
	LastError             *string                `json:"last_error"`
	LastAttemptAt         *time.Time             `json:"last_attempt_at"`
	DeliveredAt           *time.Time             `json:"delivered_at"`
	CancelledAt           *time.Time             `json:"cancelled_at"`
	CreatedAt             time.Time              `json:"created_at"`
	UpdatedAt             time.Time              `json:"updated_at"`
}

func decodeBatchClaimRow(
	reason string,
	wait time.Duration,
	deliveryRaw []byte,
	payloadID *uuid.UUID,
	body []byte,
	headersRaw []byte,
	contentType *string,
	sizeBytes *int64,
	createdAt *time.Time,
) (BatchClaimResult, error) {
	if reason != "claimed" {
		return BatchClaimResult{Reason: reason, Wait: wait}, nil
	}
	if len(deliveryRaw) == 0 || payloadID == nil || contentType == nil || sizeBytes == nil || createdAt == nil {
		return BatchClaimResult{}, errors.New("claimed delivery is missing delivery or payload data")
	}
	var encoded batchDeliveryJSON
	if err := json.Unmarshal(deliveryRaw, &encoded); err != nil {
		return BatchClaimResult{}, fmt.Errorf("decode delivery: %w", err)
	}
	var headers map[string]string
	if err := json.Unmarshal(headersRaw, &headers); err != nil {
		return BatchClaimResult{}, fmt.Errorf("decode payload headers: %w", err)
	}
	delivery := domain.Delivery{
		ID: encoded.ID, PayloadID: encoded.PayloadID, IdempotencyKey: encoded.IdempotencyKey,
		DestinationType: encoded.DestinationType, Destination: encoded.Destination,
		DeliverAt: encoded.DeliverAt.UTC(), Status: encoded.Status,
		ScheduleRevision: encoded.ScheduleRevision, HotRegisteredRevision: encoded.HotRegisteredRevision,
		Attempts: encoded.Attempts, MaxAttempts: encoded.MaxAttempts,
		ProcessingOwner: encoded.ProcessingOwner, ProcessingUntil: encoded.ProcessingUntil,
		LastError: encoded.LastError, LastAttemptAt: encoded.LastAttemptAt,
		DeliveredAt: encoded.DeliveredAt, CancelledAt: encoded.CancelledAt,
		CreatedAt: encoded.CreatedAt.UTC(), UpdatedAt: encoded.UpdatedAt.UTC(),
	}
	payload := domain.Payload{
		ID: *payloadID, Body: body, Headers: headers, ContentType: *contentType,
		SizeBytes: *sizeBytes, CreatedAt: createdAt.UTC(),
	}
	return BatchClaimResult{Delivery: &delivery, Payload: &payload, Reason: reason}, nil
}

// ClaimDelivery atomically fences a ready event by revision and takes a
// processing lease. A rejected claim is returned as classification data rather
// than a storage error so callers can ACK stale and terminal events safely.
func (s *Store) ClaimDelivery(
	ctx context.Context,
	p ClaimDeliveryParams,
) (domain.Delivery, *ClaimRejection, error) {
	if p.DeliveryID == uuid.Nil || p.ScheduleRevision <= 0 || p.Owner == "" || p.Lease <= 0 ||
		p.ClockSkewTolerance < 0 {
		return domain.Delivery{}, nil, errors.New("invalid delivery claim parameters")
	}
	ctx, cancel := s.withTimeout(ctx)
	defer cancel()
	d, err := scanDelivery(s.pool.QueryRow(ctx, `
		UPDATE deliveries
		SET status = 'processing',
			processing_owner = $1,
			processing_until = clock_timestamp() + $2 * interval '1 millisecond',
			attempts = attempts + 1,
			last_attempt_at = clock_timestamp(),
			updated_at = clock_timestamp()
		WHERE id = $3
		  AND schedule_revision = $4
		  AND status = 'scheduled'
		  AND deliver_at <= clock_timestamp() + $5 * interval '1 millisecond'
		RETURNING `+deliveryColumns,
		p.Owner, durationMillis(p.Lease), p.DeliveryID, p.ScheduleRevision,
		durationMillis(p.ClockSkewTolerance)))
	if err == nil {
		return d, nil, nil
	}
	if !errors.Is(err, domain.ErrNotFound) {
		return domain.Delivery{}, nil, fmt.Errorf("claim delivery: %w", err)
	}
	rejection, err := classifyClaimRejection(ctx, s.pool, p.DeliveryID)
	if err != nil {
		return domain.Delivery{}, nil, err
	}
	return domain.Delivery{}, &rejection, nil
}

type queryRower interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}

func classifyClaimRejection(ctx context.Context, db queryRower, id uuid.UUID) (ClaimRejection, error) {
	var result ClaimRejection
	err := db.QueryRow(ctx, `
		SELECT status, schedule_revision, deliver_at
		FROM deliveries WHERE id = $1`, id).Scan(
		&result.Status, &result.ScheduleRevision, &result.DeliverAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return result, nil
	}
	if err != nil {
		return ClaimRejection{}, fmt.Errorf("classify rejected claim: %w", err)
	}
	result.Exists = true
	result.DeliverAt = result.DeliverAt.UTC()
	return result, nil
}

// HeartbeatDelivery extends a lease only while ownership is still valid.
func (s *Store) HeartbeatDelivery(ctx context.Context, id uuid.UUID, owner string, lease time.Duration) error {
	if owner == "" || lease <= 0 {
		return errors.New("heartbeat owner and positive lease are required")
	}
	ctx, cancel := s.withTimeout(ctx)
	defer cancel()
	tag, err := s.pool.Exec(ctx, `
		UPDATE deliveries
		SET processing_until = clock_timestamp() + $3 * interval '1 millisecond',
			updated_at = clock_timestamp()
		WHERE id = $1 AND status = 'processing' AND processing_owner = $2`,
		id, owner, durationMillis(lease))
	if err != nil {
		return fmt.Errorf("heartbeat delivery: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return domain.ErrOwnershipLost
	}
	return nil
}

func (s *Store) MarkDelivered(ctx context.Context, id uuid.UUID, owner string) error {
	ctx, cancel := s.withTimeout(ctx)
	defer cancel()
	tag, err := s.pool.Exec(ctx, `
		UPDATE deliveries
		SET status = 'delivered',
			delivered_at = clock_timestamp(),
			processing_owner = NULL,
			processing_until = NULL,
			last_error = NULL,
			updated_at = clock_timestamp()
		WHERE id = $1 AND status = 'processing' AND processing_owner = $2`,
		id, owner)
	if err != nil {
		return fmt.Errorf("mark delivery successful: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return domain.ErrOwnershipLost
	}
	return nil
}

func (s *Store) ScheduleRetry(ctx context.Context, p RetryDeliveryParams) (domain.Delivery, error) {
	if p.Owner == "" || p.Delay < 0 || p.HotHorizon < 0 {
		return domain.Delivery{}, errors.New("invalid retry parameters")
	}
	ctx, cancel := s.withTimeout(ctx)
	defer cancel()
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return domain.Delivery{}, fmt.Errorf("begin delivery retry: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	d, err := scanDelivery(tx.QueryRow(ctx, `
		UPDATE deliveries
		SET status = 'scheduled',
			deliver_at = clock_timestamp() + $3 * interval '1 millisecond',
			schedule_revision = schedule_revision + 1,
			hot_registered_revision = NULL,
			processing_owner = NULL,
			processing_until = NULL,
			last_error = left($4, 2048),
			updated_at = clock_timestamp()
		WHERE id = $1 AND status = 'processing' AND processing_owner = $2
		RETURNING `+deliveryColumns,
		p.DeliveryID, p.Owner, durationMillis(p.Delay), p.LastError))
	if errors.Is(err, domain.ErrNotFound) {
		return domain.Delivery{}, domain.ErrOwnershipLost
	}
	if err != nil {
		return domain.Delivery{}, fmt.Errorf("schedule delivery retry: %w", err)
	}
	var now time.Time
	if err := tx.QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&now); err != nil {
		return domain.Delivery{}, fmt.Errorf("read database time: %w", err)
	}
	if kind, ok := outboxKindFor(d.DeliverAt, now, p.HotHorizon); ok {
		if err := insertOutbox(ctx, tx, d, kind); err != nil {
			return domain.Delivery{}, err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return domain.Delivery{}, fmt.Errorf("commit delivery retry: %w", err)
	}
	return d, nil
}

func (s *Store) MarkDead(ctx context.Context, id uuid.UUID, owner string, cause error) error {
	message := "delivery failed permanently"
	if cause != nil {
		message = cause.Error()
	}
	ctx, cancel := s.withTimeout(ctx)
	defer cancel()
	tag, err := s.pool.Exec(ctx, `
		UPDATE deliveries
		SET status = 'dead',
			processing_owner = NULL,
			processing_until = NULL,
			last_error = left($3, 2048),
			updated_at = clock_timestamp()
		WHERE id = $1 AND status = 'processing' AND processing_owner = $2`,
		id, owner, message)
	if err != nil {
		return fmt.Errorf("mark delivery dead: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return domain.ErrOwnershipLost
	}
	return nil
}

func (s *Store) DeliveryState(ctx context.Context, id uuid.UUID) (ClaimRejection, error) {
	ctx, cancel := s.withTimeout(ctx)
	defer cancel()
	return classifyClaimRejection(ctx, s.pool, id)
}
