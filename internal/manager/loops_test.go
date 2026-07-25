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
	promoter := Promoter{Repository: repo, Config: PromoterConfig{
		HotHorizon: time.Minute, BatchSize: 2, PollInterval: time.Hour, ErrorBackoff: time.Second,
	}}
	if err := promoter.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if repo.cutoffCalls != 1 || repo.batchCalls != 2 {
		t.Fatalf("unexpected calls: cutoff=%d batch=%d", repo.cutoffCalls, repo.batchCalls)
	}
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
	published   bool
}

func (*outboxRepoStub) ClaimOutbox(context.Context, string, int, time.Duration) ([]OutboxRecord, error) {
	return nil, nil
}
func (r *outboxRepoStub) MarkOutboxPublished(context.Context, OutboxRecord) error {
	r.published = true
	return nil
}
func (r *outboxRepoStub) MarkOutboxFailed(_ context.Context, _ OutboxRecord, delay time.Duration, _ string) error {
	r.failedDelay = delay
	return nil
}

type publisherStub struct{ err error }

func (p publisherStub) Publish(context.Context, natsjs.PublishRequest) error { return p.err }

func TestOutboxWorkerRecordsPublishFailure(t *testing.T) {
	repo := &outboxRepoStub{}
	worker := OutboxWorker{
		Repository: repo,
		Publisher:  publisherStub{err: errors.New("NATS unavailable")},
		Backoff:    NewBackoff(time.Second, time.Minute, 2),
	}
	err := worker.process(context.Background(), OutboxRecord{
		ID: 4, DeliveryID: uuid.New(), ScheduleRevision: 1, Kind: natsjs.OutboxReady,
		DeliverAt: time.Now(), DestinationType: domain.DestinationHTTP, PublishAttempts: 2,
	})
	if err == nil || repo.failedDelay < time.Second || repo.failedDelay > 2*time.Second || repo.published {
		t.Fatalf("failure was not persisted correctly: err=%v delay=%s published=%v", err, repo.failedDelay, repo.published)
	}
}
