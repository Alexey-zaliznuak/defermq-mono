package manager

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/defermq/defermq/internal/domain"
	"github.com/defermq/defermq/internal/hotstorage/natsjs"
	"github.com/defermq/defermq/internal/hotstorage/valkey"
	"github.com/google/uuid"
)

type fakeHotIndex struct {
	claimed    []valkey.ClaimedEntry
	registered []valkey.Entry
	completed  []string
	log        *[]string
}

func (f *fakeHotIndex) RepairRegister(_ context.Context, entry valkey.Entry) (bool, error) {
	f.registered = append(f.registered, entry)
	return true, nil
}
func (*fakeHotIndex) AcquireLease(context.Context, int, time.Duration) (string, bool, error) {
	return "token", true, nil
}
func (*fakeHotIndex) RenewLease(context.Context, int, string, time.Duration) error { return nil }
func (*fakeHotIndex) ReleaseLease(context.Context, int, string) error              { return nil }
func (f *fakeHotIndex) ClaimDue(context.Context, int, string, time.Duration, time.Duration, int) ([]valkey.ClaimedEntry, error) {
	return f.claimed, nil
}
func (*fakeHotIndex) ReclaimExpired(context.Context, int, string, int) ([]valkey.Entry, error) {
	return nil, nil
}
func (f *fakeHotIndex) Complete(_ context.Context, id uuid.UUID, revision int64, _ string) (bool, error) {
	f.completed = append(f.completed, readyKey(id.String(), revision))
	if f.log != nil {
		*f.log = append(*f.log, "complete")
	}
	return true, nil
}
func (*fakeHotIndex) Heartbeat(context.Context, int, string, string, string) (time.Time, error) {
	return time.Now(), nil
}
func (*fakeHotIndex) BucketCount() int { return 32 }

type fakeReadyRepository struct {
	records    []ReadyRecord
	marked     []ReadyRecord
	resolveErr error
	markErr    error
	log        *[]string
}

func (f *fakeReadyRepository) ResolveReady(context.Context, []valkey.ClaimedEntry) ([]ReadyRecord, error) {
	return f.records, f.resolveErr
}
func (f *fakeReadyRepository) MarkReadyPublished(_ context.Context, records []ReadyRecord) error {
	f.marked = append(f.marked, records...)
	if f.log != nil {
		*f.log = append(*f.log, "mark")
	}
	return f.markErr
}

type fakeReadyPublisher struct {
	published []natsjs.PublishRequest
	err       error
	calls     int
}

func (p *fakeReadyPublisher) PublishReadyBatch(
	_ context.Context,
	requests []natsjs.PublishRequest,
) ([]natsjs.PublishRequest, error) {
	p.calls++
	if p.published == nil && p.err == nil {
		return requests, nil
	}
	return p.published, p.err
}

func TestSchedulerMarksPostgresBeforeCompletingInflight(t *testing.T) {
	id := uuid.New()
	due := time.Now().UTC()
	log := []string{}
	index := &fakeHotIndex{
		claimed: []valkey.ClaimedEntry{{
			Entry: valkey.Entry{DeliveryID: id, Revision: 4, DueAt: due},
		}},
		log: &log,
	}
	repository := &fakeReadyRepository{records: []ReadyRecord{{
		DeliveryID: id, ScheduleRevision: 4, DeliverAt: due,
		DestinationType: domain.DestinationHTTP,
	}}, log: &log}
	var claimed, published int
	var wakeLag time.Duration
	scheduler := Scheduler{
		Index: index, Repository: repository, Publisher: &fakeReadyPublisher{},
		Config: SchedulerConfig{
			Owner: "manager-1", Worker: 0, Workers: 10, PollInterval: time.Millisecond,
			LeaseTTL: time.Second, InflightTTL: time.Second, PublishTimeout: 100 * time.Millisecond,
			EarlyWindow: 50 * time.Millisecond,
			BatchSize:   100, ReclaimBatch: 100, ErrorBackoff: time.Millisecond,
		},
		OnClaimed: func(count int) { claimed += count },
		OnPublished: func(result string) {
			if result == "success" {
				published++
			}
		},
		OnWakeLag: func(lag time.Duration) { wakeLag = lag },
	}
	if err := scheduler.runBucket(context.Background(), 0); err != nil {
		t.Fatal(err)
	}
	if len(log) != 2 || log[0] != "mark" || log[1] != "complete" {
		t.Fatalf("unexpected persistence order: %v", log)
	}
	if claimed != 1 || published != 1 || wakeLag < 0 {
		t.Fatalf("unexpected scheduler observations: claimed=%d published=%d wakeLag=%s",
			claimed, published, wakeLag)
	}
}

func TestSchedulerFailureBeforePubAckLeavesInflight(t *testing.T) {
	record, claimed := readyFixture(1)
	index := &fakeHotIndex{claimed: claimed}
	repository := &fakeReadyRepository{records: []ReadyRecord{record}}
	scheduler := testScheduler(index, repository, &fakeReadyPublisher{
		err: errors.New("scheduler stopped before PubAck"),
	})

	if err := scheduler.runBucket(context.Background(), 0); err == nil {
		t.Fatal("expected publish failure")
	}
	if len(repository.marked) != 0 || len(index.completed) != 0 {
		t.Fatalf("unacknowledged revision was finalized: marked=%v completed=%v",
			repository.marked, index.completed)
	}
}

func TestSchedulerPartialPubAckFinalizesOnlySuccesses(t *testing.T) {
	first, firstClaim := readyFixture(3)
	second, secondClaim := readyFixture(4)
	index := &fakeHotIndex{claimed: append(firstClaim, secondClaim...)}
	repository := &fakeReadyRepository{records: []ReadyRecord{first, second}}
	publisher := &fakeReadyPublisher{
		published: []natsjs.PublishRequest{readyRequest(first)},
		err:       errors.New("second PubAck failed"),
	}
	scheduler := testScheduler(index, repository, publisher)

	if err := scheduler.runBucket(context.Background(), 0); err == nil {
		t.Fatal("expected partial publish failure")
	}
	if len(repository.marked) != 1 || repository.marked[0].DeliveryID != first.DeliveryID {
		t.Fatalf("wrong PG publications marked: %+v", repository.marked)
	}
	if len(index.completed) != 1 ||
		index.completed[0] != readyKey(first.DeliveryID.String(), first.ScheduleRevision) {
		t.Fatalf("wrong inflight revisions completed: %v", index.completed)
	}
}

func TestSchedulerMarkReadyPublishedFailureLeavesInflight(t *testing.T) {
	record, claimed := readyFixture(5)
	index := &fakeHotIndex{claimed: claimed}
	repository := &fakeReadyRepository{
		records: []ReadyRecord{record},
		markErr: errors.New("PostgreSQL unavailable"),
	}
	scheduler := testScheduler(index, repository, &fakeReadyPublisher{})

	if err := scheduler.runBucket(context.Background(), 0); err == nil {
		t.Fatal("expected PostgreSQL mark failure")
	}
	if len(index.completed) != 0 {
		t.Fatalf("revision completed despite PG failure: %v", index.completed)
	}
}

func TestSchedulerCompletesStaleRevisionWithoutPublish(t *testing.T) {
	_, claimed := readyFixture(6)
	index := &fakeHotIndex{claimed: claimed}
	repository := &fakeReadyRepository{}
	publisher := &fakeReadyPublisher{}
	scheduler := testScheduler(index, repository, publisher)

	if err := scheduler.runBucket(context.Background(), 0); err != nil {
		t.Fatal(err)
	}
	if publisher.calls != 0 || len(repository.marked) != 0 || len(index.completed) != 1 {
		t.Fatalf("stale revision handling: publish calls=%d marked=%v completed=%v",
			publisher.calls, repository.marked, index.completed)
	}
}

func TestSchedulerRejectsUnknownPubAck(t *testing.T) {
	record, claimed := readyFixture(7)
	index := &fakeHotIndex{claimed: claimed}
	repository := &fakeReadyRepository{records: []ReadyRecord{record}}
	unknown := record
	unknown.DeliveryID = uuid.New()
	scheduler := testScheduler(index, repository, &fakeReadyPublisher{
		published: []natsjs.PublishRequest{readyRequest(unknown)},
	})

	if err := scheduler.runBucket(context.Background(), 0); err == nil {
		t.Fatal("expected invalid PubAck failure")
	}
	if len(repository.marked) != 0 || len(index.completed) != 0 {
		t.Fatalf("unknown PubAck finalized state: marked=%v completed=%v",
			repository.marked, index.completed)
	}
}

func TestSchedulerRequiresInflightTTLAbovePublishTimeout(t *testing.T) {
	scheduler := testScheduler(
		&fakeHotIndex{},
		&fakeReadyRepository{},
		&fakeReadyPublisher{},
	)
	scheduler.Config.InflightTTL = scheduler.Config.PublishTimeout
	if err := scheduler.Run(context.Background()); err == nil {
		t.Fatal("accepted inflight TTL that cannot cover publish timeout")
	}
}

func readyFixture(revision int64) (ReadyRecord, []valkey.ClaimedEntry) {
	record := ReadyRecord{
		DeliveryID:       uuid.New(),
		ScheduleRevision: revision,
		DeliverAt:        time.Now().UTC(),
		DestinationType:  domain.DestinationHTTP,
	}
	return record, []valkey.ClaimedEntry{{
		Entry: valkey.Entry{
			DeliveryID: record.DeliveryID,
			Revision:   record.ScheduleRevision,
			DueAt:      record.DeliverAt,
		},
	}}
}

func readyRequest(record ReadyRecord) natsjs.PublishRequest {
	return natsjs.PublishRequest{
		Kind:             natsjs.OutboxReady,
		DeliveryID:       record.DeliveryID,
		ScheduleRevision: record.ScheduleRevision,
		DeliverAt:        record.DeliverAt,
		DestinationType:  record.DestinationType,
	}
}

func testScheduler(
	index HotIndex,
	repository ReadyRepository,
	publisher ReadyBatchPublisher,
) Scheduler {
	return Scheduler{
		Index: index, Repository: repository, Publisher: publisher,
		Config: SchedulerConfig{
			Owner: "manager-1", Worker: 0, Workers: 1,
			PollInterval: time.Millisecond, LeaseTTL: time.Second,
			InflightTTL: time.Second, PublishTimeout: 100 * time.Millisecond,
			BatchSize: 100, ReclaimBatch: 100, ErrorBackoff: time.Millisecond,
		},
	}
}

type fakeRegistrarRepository struct {
	record         OutboxRecord
	published      bool
	publishedBatch []OutboxRecord
	failed         []OutboxRecord
}

func (f *fakeRegistrarRepository) ClaimOutbox(context.Context, string, natsjs.OutboxKind, int, time.Duration) ([]OutboxRecord, error) {
	return nil, nil
}
func (f *fakeRegistrarRepository) MarkOutboxPublished(context.Context, OutboxRecord) error {
	f.published = true
	return nil
}
func (f *fakeRegistrarRepository) MarkOutboxFailed(
	_ context.Context,
	record OutboxRecord,
	_ time.Duration,
	_ string,
) error {
	f.failed = append(f.failed, record)
	return nil
}

type fakeBatchRegistrarRepository struct {
	fakeRegistrarRepository
}

func (f *fakeBatchRegistrarRepository) MarkOutboxPublishedBatch(
	_ context.Context,
	records []OutboxRecord,
) error {
	f.publishedBatch = append(f.publishedBatch, records...)
	return nil
}

type fakeBatchHotIndex struct {
	fakeHotIndex
	results []valkey.RepairRegisterResult
	err     error
	batches [][]valkey.Entry
}

func (f *fakeBatchHotIndex) RepairRegisterBatch(
	_ context.Context,
	entries []valkey.Entry,
) ([]valkey.RepairRegisterResult, error) {
	f.batches = append(f.batches, append([]valkey.Entry(nil), entries...))
	return f.results, f.err
}

func TestRegistrarRegistersBeforeCompletingOutbox(t *testing.T) {
	record := OutboxRecord{
		ID: 1, DeliveryID: uuid.New(), ScheduleRevision: 2,
		Kind: natsjs.OutboxHotRegister, DeliverAt: time.Now().Add(time.Second),
	}
	repository := &fakeRegistrarRepository{record: record}
	index := &fakeHotIndex{}
	var result string
	registrar := Registrar{Repository: repository, Index: index, Backoff: NewBackoff(
		time.Millisecond, time.Second, 1,
	), OnZADD: func(value string) { result = value }}
	if err := registrar.register(context.Background(), record); err != nil {
		t.Fatal(err)
	}
	if !repository.published || len(index.registered) != 1 ||
		index.registered[0].Revision != record.ScheduleRevision {
		t.Fatalf("registration was not durably completed: published=%v entries=%v",
			repository.published, index.registered)
	}
	if result != "inserted" {
		t.Fatalf("ZADD result = %q, want inserted", result)
	}
}

func TestRegistrarCompletesSuccessfulRegistrationsAsBatch(t *testing.T) {
	records := []OutboxRecord{
		{ID: 1, DeliveryID: uuid.New(), ScheduleRevision: 2, Kind: natsjs.OutboxHotRegister,
			DeliverAt: time.Now().Add(time.Second)},
		{ID: 2, DeliveryID: uuid.New(), ScheduleRevision: 3, Kind: natsjs.OutboxHotRegister,
			DeliverAt: time.Now().Add(2 * time.Second)},
	}
	repository := &fakeBatchRegistrarRepository{}
	index := &fakeHotIndex{}
	registrar := Registrar{Repository: repository, Index: index, Backoff: NewBackoff(
		time.Millisecond, time.Second, 1,
	)}

	if err := registrar.registerBatch(context.Background(), records); err != nil {
		t.Fatal(err)
	}
	if repository.published || len(repository.publishedBatch) != len(records) {
		t.Fatalf("registrations were not batch completed: single=%v batch=%v",
			repository.published, repository.publishedBatch)
	}
}

func TestRegistrarBatchReleasesOnlyFailedBucketRecords(t *testing.T) {
	records := []OutboxRecord{
		{ID: 1, DeliveryID: uuid.New(), ScheduleRevision: 1, DeliverAt: time.Now().Add(time.Minute)},
		{ID: 2, DeliveryID: uuid.New(), ScheduleRevision: 1, DeliverAt: time.Now().Add(2 * time.Minute)},
	}
	bucketErr := errors.New("bucket unavailable")
	index := &fakeBatchHotIndex{
		results: []valkey.RepairRegisterResult{
			{Inserted: true},
			{Err: bucketErr},
		},
		err: bucketErr,
	}
	repository := &fakeBatchRegistrarRepository{}
	var outcomes []string
	registrar := Registrar{
		Repository: repository,
		Index:      index,
		Backoff:    NewBackoff(time.Millisecond, time.Second, 1),
		OnZADD:     func(result string) { outcomes = append(outcomes, result) },
	}

	if err := registrar.registerBatch(context.Background(), records); err == nil {
		t.Fatal("expected partial bucket failure")
	}
	if len(index.batches) != 1 || len(index.batches[0]) != 2 {
		t.Fatalf("batch calls = %+v", index.batches)
	}
	if len(repository.publishedBatch) != 1 || repository.publishedBatch[0].ID != records[0].ID {
		t.Fatalf("published records = %+v", repository.publishedBatch)
	}
	if len(repository.failed) != 1 || repository.failed[0].ID != records[1].ID {
		t.Fatalf("released records = %+v", repository.failed)
	}
	if len(outcomes) != 2 || outcomes[0] != "inserted" || outcomes[1] != "error" {
		t.Fatalf("outcomes = %v", outcomes)
	}
}

func TestRepairerUsesBatchOutcomesForPage(t *testing.T) {
	index := &fakeBatchHotIndex{
		results: []valkey.RepairRegisterResult{
			{Inserted: true},
			{Inserted: false},
		},
	}
	var outcomes []string
	repairer := Repairer{
		Index:      index,
		OnRegister: func(result string) { outcomes = append(outcomes, result) },
	}
	entries := []valkey.Entry{
		{DeliveryID: uuid.New(), Revision: 1, DueAt: time.Now().Add(time.Minute)},
		{DeliveryID: uuid.New(), Revision: 2, DueAt: time.Now().Add(2 * time.Minute)},
	}

	if err := repairer.registerPage(context.Background(), entries); err != nil {
		t.Fatal(err)
	}
	if len(index.batches) != 1 || len(index.registered) != 0 {
		t.Fatalf("batch calls=%d single registrations=%d", len(index.batches), len(index.registered))
	}
	if len(outcomes) != 2 || outcomes[0] != "inserted" || outcomes[1] != "existing" {
		t.Fatalf("outcomes = %v", outcomes)
	}
}
