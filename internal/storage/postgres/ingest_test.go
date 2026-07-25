package postgres

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/defermq/defermq/internal/domain"
	"github.com/defermq/defermq/internal/ingest"
	"github.com/google/uuid"
)

func TestIntegrationIngestBatchOrderingAndDuplicateReplay(t *testing.T) {
	store := integrationStore(t)
	id := uuid.New()
	payloadID := uuid.New()
	key := "ingest-batch-order"
	deliverAt := time.Now().Add(time.Hour).UTC()
	create := ingest.Command{
		SchemaVersion: ingest.SchemaVersion, Kind: ingest.KindCreate,
		CommandID: uuid.New(), DeliveryID: id, PayloadID: payloadID,
		IdempotencyKey: &key, DestinationType: domain.DestinationHTTP,
		Destination: json.RawMessage(`{"type":"http","http":{"url":"https://example.com"}}`),
		DeliverAt:   deliverAt, MaxAttempts: 3, HotHorizon: 2 * time.Hour,
		Payload: &ingest.Payload{
			Body: []byte("body"), Headers: map[string]string{"X-Test": "yes"},
			ContentType: "text/plain", SizeBytes: 4,
		},
	}
	reschedule := ingest.Command{
		SchemaVersion: ingest.SchemaVersion, Kind: ingest.KindReschedule,
		CommandID: uuid.New(), DeliveryID: id, DeliverAt: deliverAt.Add(time.Hour),
		HotHorizon: 3 * time.Hour, ExpectedRevision: 1,
	}
	cancel := ingest.Command{
		SchemaVersion: ingest.SchemaVersion, Kind: ingest.KindCancel,
		CommandID: uuid.New(), DeliveryID: id, ExpectedRevision: 2,
	}
	batch := []ingest.Command{create, reschedule, cancel}
	if err := store.ApplyIngestBatch(context.Background(), batch); err != nil {
		t.Fatal(err)
	}
	if err := store.ApplyIngestBatch(context.Background(), batch); err != nil {
		t.Fatal(err)
	}
	delivery, err := store.GetDelivery(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	if delivery.Status != domain.StatusCancelled || delivery.ScheduleRevision != 3 {
		t.Fatalf("duplicate replay changed final state: %#v", delivery)
	}
	var payloadCount int
	if err := store.pool.QueryRow(context.Background(),
		`SELECT count(*) FROM message_payloads`).Scan(&payloadCount); err != nil {
		t.Fatal(err)
	}
	if payloadCount != 1 {
		t.Fatalf("batch leaked payloads: %d", payloadCount)
	}
}
