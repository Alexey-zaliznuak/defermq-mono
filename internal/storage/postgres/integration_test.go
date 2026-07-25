package postgres

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/defermq/defermq/internal/domain"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func integrationStore(t *testing.T) *Store {
	t.Helper()
	dsn := os.Getenv("DEFERMQ_TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("set DEFERMQ_TEST_POSTGRES_DSN to run PostgreSQL integration tests")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	admin, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("connect integration database: %v", err)
	}
	schema := "defermq_test_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	if _, err := admin.Exec(ctx, "CREATE SCHEMA "+pgx.Identifier{schema}.Sanitize()); err != nil {
		admin.Close()
		t.Fatalf("create test schema: %v", err)
	}
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatalf("parse test DSN: %v", err)
	}
	cfg.ConnConfig.RuntimeParams["search_path"] = schema
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatalf("create isolated test pool: %v", err)
	}
	store := New(pool, 10*time.Second)
	if err := store.Migrate(ctx); err != nil {
		store.Close()
		t.Fatalf("migrate isolated test schema: %v", err)
	}
	t.Cleanup(func() {
		store.Close()
		dropCtx, dropCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer dropCancel()
		_, _ = admin.Exec(dropCtx, "DROP SCHEMA "+pgx.Identifier{schema}.Sanitize()+" CASCADE")
		admin.Close()
	})
	return store
}

func newCreateParams(deliverAt time.Time, key *string) CreateDeliveryParams {
	payloadID := uuid.New()
	return CreateDeliveryParams{
		Delivery: domain.Delivery{
			ID:              uuid.New(),
			PayloadID:       payloadID,
			IdempotencyKey:  key,
			DestinationType: domain.DestinationHTTP,
			Destination:     []byte(`{"type":"http","http":{"url":"https://example.com"}}`),
			DeliverAt:       deliverAt,
			MaxAttempts:     3,
		},
		Payload: domain.Payload{
			ID: payloadID, Body: []byte(`{"ok":true}`), Headers: map[string]string{"X-Test": "yes"},
			ContentType: "application/json", SizeBytes: 11,
		},
		HotHorizon: time.Minute,
	}
}

func TestIntegrationGatewayAndPusherLifecycle(t *testing.T) {
	store := integrationStore(t)
	ctx := context.Background()
	key := "integration-idempotency"
	params := newCreateParams(time.Now().Add(-5*time.Second), &key)
	created, replay, err := store.CreateDelivery(ctx, params)
	if err != nil || replay {
		t.Fatalf("CreateDelivery() = replay %v, error %v", replay, err)
	}
	if created.Status != domain.StatusScheduled || created.ScheduleRevision != 1 {
		t.Fatalf("unexpected created delivery: %#v", created)
	}

	replayParams := newCreateParams(time.Now(), &key)
	replayed, replay, err := store.CreateDelivery(ctx, replayParams)
	if err != nil || !replay || replayed.ID != created.ID {
		t.Fatalf("idempotent replay = %#v, %v, %v", replayed, replay, err)
	}
	var payloadCount int
	if err := store.pool.QueryRow(ctx, `SELECT count(*) FROM message_payloads`).Scan(&payloadCount); err != nil {
		t.Fatal(err)
	}
	if payloadCount != 1 {
		t.Fatalf("idempotency conflict leaked payload: count=%d", payloadCount)
	}

	items, err := store.ClaimOutbox(ctx, "manager-1", 10, time.Minute)
	if err != nil || len(items) != 1 || items[0].Kind != OutboxReady {
		t.Fatalf("ClaimOutbox() = %#v, %v", items, err)
	}
	if err := store.MarkOutboxPublished(ctx, items[0].ID, "manager-1"); err != nil {
		t.Fatalf("MarkOutboxPublished(): %v", err)
	}

	claimed, rejection, err := store.ClaimDelivery(ctx, ClaimDeliveryParams{
		DeliveryID: created.ID, ScheduleRevision: 1, Owner: "pusher-1",
		Lease: time.Minute,
	})
	if err != nil || rejection != nil || claimed.Attempts != 1 {
		t.Fatalf("ClaimDelivery() = %#v, %#v, %v", claimed, rejection, err)
	}
	payload, err := store.GetPayload(ctx, claimed.PayloadID, 1024)
	if err != nil || string(payload.Body) != `{"ok":true}` {
		t.Fatalf("GetPayload() = %#v, %v", payload, err)
	}
	if err := store.HeartbeatDelivery(ctx, created.ID, "pusher-1", time.Minute); err != nil {
		t.Fatalf("HeartbeatDelivery(): %v", err)
	}
	retried, err := store.ScheduleRetry(ctx, RetryDeliveryParams{
		DeliveryID: created.ID, Owner: "pusher-1", Delay: 10 * time.Millisecond,
		HotHorizon: time.Minute, LastError: "temporary",
	})
	if err != nil || retried.ScheduleRevision != 2 || retried.Status != domain.StatusScheduled {
		t.Fatalf("ScheduleRetry() = %#v, %v", retried, err)
	}
	_, stale, err := store.ClaimDelivery(ctx, ClaimDeliveryParams{
		DeliveryID: created.ID, ScheduleRevision: 1, Owner: "pusher-stale", Lease: time.Minute,
		ClockSkewTolerance: time.Second,
	})
	if err != nil || stale == nil || !stale.Exists || stale.ScheduleRevision != 2 {
		t.Fatalf("stale claim classification = %#v, %v", stale, err)
	}
	claimed, rejection, err = store.ClaimDelivery(ctx, ClaimDeliveryParams{
		DeliveryID: created.ID, ScheduleRevision: 2, Owner: "pusher-2", Lease: time.Minute,
		ClockSkewTolerance: time.Second,
	})
	if err != nil || rejection != nil {
		t.Fatalf("second ClaimDelivery() = %#v, %#v, %v", claimed, rejection, err)
	}
	if err := store.MarkDelivered(ctx, created.ID, "pusher-2"); err != nil {
		t.Fatalf("MarkDelivered(): %v", err)
	}
	final, err := store.GetDelivery(ctx, created.ID)
	if err != nil || final.Status != domain.StatusDelivered || final.Attempts != 2 {
		t.Fatalf("final delivery = %#v, %v", final, err)
	}
}

func TestIntegrationClaimDeliveriesBatchOutcomesAndOrder(t *testing.T) {
	store := integrationStore(t)
	ctx := context.Background()
	now := time.Now()

	claimable, _, err := store.CreateDelivery(ctx, newCreateParams(now.Add(-time.Second), nil))
	if err != nil {
		t.Fatal(err)
	}
	early, _, err := store.CreateDelivery(ctx, newCreateParams(now.Add(time.Minute), nil))
	if err != nil {
		t.Fatal(err)
	}
	stale, _, err := store.CreateDelivery(ctx, newCreateParams(now.Add(-time.Second), nil))
	if err != nil {
		t.Fatal(err)
	}
	stale, err = store.RescheduleDelivery(ctx, RescheduleParams{
		DeliveryID: stale.ID, DeliverAt: now.Add(time.Minute), HotHorizon: time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}
	terminal, _, err := store.CreateDelivery(ctx, newCreateParams(now.Add(-time.Second), nil))
	if err != nil {
		t.Fatal(err)
	}
	terminal, err = store.CancelDelivery(ctx, terminal.ID)
	if err != nil {
		t.Fatal(err)
	}

	results, err := store.ClaimDeliveriesBatch(ctx, []ClaimRequest{
		{DeliveryID: early.ID, ScheduleRevision: early.ScheduleRevision},
		{DeliveryID: claimable.ID, ScheduleRevision: claimable.ScheduleRevision},
		{DeliveryID: stale.ID, ScheduleRevision: stale.ScheduleRevision - 1},
		{DeliveryID: terminal.ID, ScheduleRevision: terminal.ScheduleRevision},
	}, "batch-pusher", time.Minute, 10*time.Millisecond)
	if err != nil {
		t.Fatalf("ClaimDeliveriesBatch(): %v", err)
	}
	wantReasons := []string{"too_early", "claimed", "stale_revision", "terminal"}
	for index, want := range wantReasons {
		if results[index].Reason != want {
			t.Errorf("result[%d].Reason = %q, want %q", index, results[index].Reason, want)
		}
	}
	if results[0].Wait <= 0 {
		t.Errorf("early wait = %s", results[0].Wait)
	}
	if results[1].Delivery == nil || results[1].Delivery.ID != claimable.ID ||
		results[1].Delivery.Attempts != 1 || results[1].Payload == nil ||
		string(results[1].Payload.Body) != `{"ok":true}` {
		t.Fatalf("claimed result did not include delivery and payload: %#v", results[1])
	}

	duplicate, _, err := store.CreateDelivery(ctx, newCreateParams(now.Add(-time.Second), nil))
	if err != nil {
		t.Fatal(err)
	}
	duplicates, err := store.ClaimDeliveriesBatch(ctx, []ClaimRequest{
		{DeliveryID: duplicate.ID, ScheduleRevision: duplicate.ScheduleRevision},
		{DeliveryID: duplicate.ID, ScheduleRevision: duplicate.ScheduleRevision},
	}, "batch-pusher", time.Minute, 0)
	if err != nil {
		t.Fatal(err)
	}
	if duplicates[0].Reason != "claimed" || duplicates[1].Reason != "processing" {
		t.Fatalf("duplicate outcomes = %q, %q", duplicates[0].Reason, duplicates[1].Reason)
	}
}

func TestIntegrationManagerMaintenance(t *testing.T) {
	store := integrationStore(t)
	ctx := context.Background()

	far := newCreateParams(time.Now().Add(10*time.Minute), nil)
	created, _, err := store.CreateDelivery(ctx, far)
	if err != nil {
		t.Fatal(err)
	}
	result, err := store.PromoteBatch(ctx, time.Now().Add(11*time.Minute), 1)
	if err != nil || result.Candidates != 1 || result.Inserted != 1 {
		t.Fatalf("PromoteBatch() = %#v, %v", result, err)
	}
	result, err = store.PromoteBatch(ctx, time.Now().Add(11*time.Minute), 1)
	if err != nil || result.Candidates != 0 {
		t.Fatalf("existing outbox was promoted again: %#v, %v", result, err)
	}

	if _, err := store.pool.Exec(ctx, `
		UPDATE deliveries
		SET status = 'processing', processing_owner = 'dead-worker',
			processing_until = clock_timestamp() - interval '1 second'
		WHERE id = $1`, created.ID); err != nil {
		t.Fatal(err)
	}
	reaped, err := store.ReapExpiredLeases(ctx, 0, 10)
	if err != nil || reaped != 1 {
		t.Fatalf("ReapExpiredLeases() = %d, %v", reaped, err)
	}
	recovered, err := store.GetDelivery(ctx, created.ID)
	if err != nil || recovered.Status != domain.StatusScheduled || recovered.ScheduleRevision != 2 {
		t.Fatalf("recovered delivery = %#v, %v", recovered, err)
	}

	metrics, err := store.CollectManagerDBMetrics(ctx)
	if err != nil || metrics.ScheduledDue < 1 || metrics.Outbox[OutboxReady].Pending < 1 {
		t.Fatalf("CollectManagerDBMetrics() = %#v, %v", metrics, err)
	}
}

func TestIntegrationRecoveryQueriesFenceCompletedAndStaleRevisions(t *testing.T) {
	store := integrationStore(t)
	ctx := context.Background()

	overdue, _, err := store.CreateDelivery(ctx, newCreateParams(time.Now().Add(-time.Minute), nil))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.pool.Exec(ctx,
		`DELETE FROM nats_outbox WHERE delivery_id = $1`, overdue.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := store.pool.Exec(ctx, `
		UPDATE deliveries
		SET ready_published_revision = schedule_revision,
			ready_published_at = clock_timestamp()
		WHERE id = $1`, overdue.ID); err != nil {
		t.Fatal(err)
	}
	if reconciled, err := store.ReconcileOverdue(ctx, 0, 10); err != nil || reconciled != 0 {
		t.Fatalf("ready-published revision reconciled again: count=%d err=%v", reconciled, err)
	}

	pastPromotion, _, err := store.CreateDelivery(
		ctx, newCreateParams(time.Now().Add(10*time.Minute), nil),
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.pool.Exec(ctx, `
		UPDATE deliveries SET deliver_at = clock_timestamp() - interval '1 second'
		WHERE id = $1`, pastPromotion.ID); err != nil {
		t.Fatal(err)
	}
	promotion, err := store.PromoteBatch(ctx, time.Now().Add(time.Hour), 10)
	if err != nil || promotion.Candidates != 0 || promotion.Inserted != 0 {
		t.Fatalf("promoter re-registered overdue revision: result=%+v err=%v", promotion, err)
	}

	rescheduled, _, err := store.CreateDelivery(
		ctx, newCreateParams(time.Now().Add(10*time.Minute), nil),
	)
	if err != nil {
		t.Fatal(err)
	}
	rescheduled, err = store.RescheduleDelivery(ctx, RescheduleParams{
		DeliveryID: rescheduled.ID,
		DeliverAt:  time.Now().Add(-5 * time.Second),
		HotHorizon: time.Minute,
	})
	if err != nil || rescheduled.ScheduleRevision != 2 {
		t.Fatalf("RescheduleDelivery() = %+v, %v", rescheduled, err)
	}
	if _, rejection, err := store.ClaimDelivery(ctx, ClaimDeliveryParams{
		DeliveryID: rescheduled.ID, ScheduleRevision: 1, Owner: "stale-reschedule",
		Lease: time.Minute,
	}); err != nil || rejection == nil || rejection.ScheduleRevision != 2 {
		t.Fatalf("old rescheduled revision was not fenced: rejection=%+v err=%v", rejection, err)
	}
	if _, rejection, err := store.ClaimDelivery(ctx, ClaimDeliveryParams{
		DeliveryID: rescheduled.ID, ScheduleRevision: 2, Owner: "current-reschedule",
		Lease: time.Minute, ClockSkewTolerance: 5 * time.Second,
	}); err != nil || rejection != nil {
		t.Fatalf("current rescheduled revision was rejected: rejection=%+v err=%v", rejection, err)
	}

	cancelled, _, err := store.CreateDelivery(
		ctx, newCreateParams(time.Now().Add(-time.Second), nil),
	)
	if err != nil {
		t.Fatal(err)
	}
	cancelled, err = store.CancelDelivery(ctx, cancelled.ID)
	if err != nil || cancelled.ScheduleRevision != 2 || cancelled.Status != domain.StatusCancelled {
		t.Fatalf("CancelDelivery() = %+v, %v", cancelled, err)
	}
	if _, rejection, err := store.ClaimDelivery(ctx, ClaimDeliveryParams{
		DeliveryID: cancelled.ID, ScheduleRevision: 1, Owner: "stale-cancel",
		Lease: time.Minute,
	}); err != nil || rejection == nil || rejection.ScheduleRevision != 2 ||
		rejection.Status != domain.StatusCancelled {
		t.Fatalf("old cancelled revision was not fenced: rejection=%+v err=%v", rejection, err)
	}
}
