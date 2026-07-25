package manager

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/defermq/defermq/internal/domain"
	"github.com/defermq/defermq/internal/hotstorage/natsjs"
	"github.com/google/uuid"
)

type promoterRepoStub struct {
	cancel      context.CancelFunc
	cutoffCalls int
	batchCalls  int
}

func (r *promoterRepoStub) PromotionCutoff(context.Context, time.Duration) (time.Time, error) {
	r.cutoffCalls++
	return time.Now(), nil
}

func (r *promoterRepoStub) PromoteBatch(context.Context, time.Time, int) (PromotionResult, error) {
	r.batchCalls++
	if r.batchCalls == 1 {
		return PromotionResult{Candidates: 2, Created: 2}, nil
	}
	r.cancel()
	return PromotionResult{Candidates: 1, Created: 1}, nil
}

func TestPromoterImmediatelyContinuesFullBatch(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	repo := &promoterRepoStub{cancel: cancel}
	var observations []loopObservation
	promoter := Promoter{Repository: repo, Config: PromoterConfig{
		HotHorizon: time.Minute, BatchSize: 2, PollInterval: time.Hour, ErrorBackoff: time.Second,
	}, Observe: func(component string, duration time.Duration, succeeded, fullBatch bool) {
		observations = append(observations, loopObservation{
			component: component, duration: duration, succeeded: succeeded, fullBatch: fullBatch,
		})
	}}
	if err := promoter.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if repo.cutoffCalls != 1 || repo.batchCalls != 2 {
		t.Fatalf("unexpected calls: cutoff=%d batch=%d", repo.cutoffCalls, repo.batchCalls)
	}
	if len(observations) != 1 || observations[0].component != "promoter" ||
		!observations[0].succeeded || !observations[0].fullBatch || observations[0].duration < 0 {
		t.Fatalf("unexpected promoter cycle observations: %+v", observations)
	}
}

type loopObservation struct {
	component string
	duration  time.Duration
	succeeded bool
	fullBatch bool
}

func TestBackoffUsesBoundedEqualJitter(t *testing.T) {
	backoff := NewBackoff(time.Second, 8*time.Second, 1)
	for attempt := 1; attempt <= 20; attempt++ {
		delay := backoff.Delay(attempt)
		base := time.Second << min(attempt-1, 3)
		if delay < base/2 || delay > base {
			t.Fatalf("attempt %d delay %s outside [%s,%s]", attempt, delay, base/2, base)
		}
	}
}

type outboxRepoStub struct {
	failedDelay time.Duration
	failed      []OutboxRecord
	published   bool
}

func (*outboxRepoStub) ClaimOutbox(context.Context, string, natsjs.OutboxKind, int, time.Duration) ([]OutboxRecord, error) {
	return nil, nil
}
func (r *outboxRepoStub) MarkOutboxPublished(context.Context, OutboxRecord) error {
	r.published = true
	return nil
}
func (r *outboxRepoStub) MarkOutboxFailed(_ context.Context, record OutboxRecord, delay time.Duration, _ string) error {
	r.failedDelay = delay
	r.failed = append(r.failed, record)
	return nil
}

type publisherStub struct{ err error }

func (p publisherStub) Publish(context.Context, natsjs.PublishRequest) error { return p.err }

func TestOutboxWorkerRecordsPublishFailure(t *testing.T) {
	repo := &outboxRepoStub{}
	var observedKind natsjs.OutboxKind
	var observedDuration time.Duration
	worker := OutboxWorker{
		Repository: repo,
		Publisher:  publisherStub{err: errors.New("NATS unavailable")},
		Backoff:    NewBackoff(time.Second, time.Minute, 2),
		OnPublish: func(kind natsjs.OutboxKind, duration time.Duration) {
			observedKind = kind
			observedDuration = duration
		},
	}
	err := worker.process(context.Background(), OutboxRecord{
		ID: 4, DeliveryID: uuid.New(), ScheduleRevision: 1, Kind: natsjs.OutboxReady,
		DeliverAt: time.Now(), DestinationType: domain.DestinationHTTP, PublishAttempts: 2,
	})
	if err == nil || repo.failedDelay < time.Second || repo.failedDelay > 2*time.Second || repo.published {
		t.Fatalf("failure was not persisted correctly: err=%v delay=%s published=%v", err, repo.failedDelay, repo.published)
	}
	if observedKind != natsjs.OutboxReady || observedDuration < 0 {
		t.Fatalf("publish duration was not observed: kind=%s duration=%s", observedKind, observedDuration)
	}
}

type batchOutboxRepoStub struct {
	outboxRepoStub
	publishedBatch []OutboxRecord
}

func (r *batchOutboxRepoStub) MarkOutboxPublishedBatch(
	_ context.Context,
	records []OutboxRecord,
) error {
	r.publishedBatch = append(r.publishedBatch, records...)
	return nil
}

type readyBatchPublisherStub struct {
	published []natsjs.PublishRequest
	err       error
}

func (*readyBatchPublisherStub) Publish(context.Context, natsjs.PublishRequest) error {
	return errors.New("unexpected single publish")
}

func (p *readyBatchPublisherStub) PublishReadyBatch(
	_ context.Context,
	_ []natsjs.PublishRequest,
) ([]natsjs.PublishRequest, error) {
	return p.published, p.err
}

func TestOutboxWorkerBatchCompletesAcksAndReleasesFailures(t *testing.T) {
	first := OutboxRecord{
		ID: 1, LockedBy: "worker", DeliveryID: uuid.New(), ScheduleRevision: 1,
		Kind: natsjs.OutboxReady, DeliverAt: time.Now(), DestinationType: domain.DestinationHTTP,
		PublishAttempts: 1,
	}
	second := OutboxRecord{
		ID: 2, LockedBy: "worker", DeliveryID: uuid.New(), ScheduleRevision: 2,
		Kind: natsjs.OutboxReady, DeliverAt: time.Now(), DestinationType: domain.DestinationHTTP,
		PublishAttempts: 2,
	}
	repo := &batchOutboxRepoStub{}
	publisher := &readyBatchPublisherStub{
		published: []natsjs.PublishRequest{publishRequest(first)},
		err:       errors.New("second PubAck failed"),
	}
	worker := OutboxWorker{
		Repository: repo,
		Publisher:  publisher,
		Backoff:    NewBackoff(time.Second, time.Minute, 2),
	}

	err := worker.processBatch(context.Background(), []OutboxRecord{first, second})
	if err == nil {
		t.Fatal("expected partial batch failure")
	}
	if repo.published || len(repo.publishedBatch) != 1 || repo.publishedBatch[0].ID != first.ID {
		t.Fatalf("unexpected completed records: single=%v batch=%v", repo.published, repo.publishedBatch)
	}
	if len(repo.failed) != 1 || repo.failed[0].ID != second.ID {
		t.Fatalf("unexpected released records: %v", repo.failed)
	}
}

func TestOutboxWorkerFallbackPublisherStillCompletesBatch(t *testing.T) {
	records := []OutboxRecord{
		{ID: 1, DeliveryID: uuid.New(), ScheduleRevision: 1, Kind: natsjs.OutboxReady,
			DeliverAt: time.Now(), DestinationType: domain.DestinationHTTP},
		{ID: 2, DeliveryID: uuid.New(), ScheduleRevision: 1, Kind: natsjs.OutboxReady,
			DeliverAt: time.Now(), DestinationType: domain.DestinationHTTP},
	}
	repo := &batchOutboxRepoStub{}
	worker := OutboxWorker{
		Repository: repo,
		Publisher:  publisherStub{},
		Backoff:    NewBackoff(time.Second, time.Minute, 2),
	}

	if err := worker.processBatch(context.Background(), records); err != nil {
		t.Fatal(err)
	}
	if repo.published || len(repo.publishedBatch) != len(records) {
		t.Fatalf("fallback publications were not batch completed: single=%v batch=%v",
			repo.published, repo.publishedBatch)
	}
}
