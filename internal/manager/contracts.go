package manager

import (
	"context"
	"math"
	"math/rand"
	"sync"
	"time"

	"github.com/defermq/defermq/internal/domain"
	"github.com/defermq/defermq/internal/hotstorage/natsjs"
	"github.com/defermq/defermq/internal/hotstorage/valkey"
	"github.com/google/uuid"
)

type PromotionResult struct {
	Candidates int
	Created    int
}

type OutboxRecord struct {
	ID               int64
	LockedBy         string
	DeliveryID       uuid.UUID
	ScheduleRevision int64
	Kind             natsjs.OutboxKind
	DeliverAt        time.Time
	DestinationType  domain.DestinationType
	PublishAttempts  int
}

type PromoterRepository interface {
	PromotionCutoff(context.Context, time.Duration) (time.Time, error)
	PromoteBatch(context.Context, time.Time, int) (PromotionResult, error)
}

type OutboxRepository interface {
	ClaimOutbox(context.Context, string, natsjs.OutboxKind, int, time.Duration) ([]OutboxRecord, error)
	MarkOutboxPublished(context.Context, OutboxRecord) error
	MarkOutboxFailed(context.Context, OutboxRecord, time.Duration, string) error
}

// OutboxBatchRepository is an optional optimized completion path. Keeping it
// separate from OutboxRepository preserves simple repositories and test fakes.
type OutboxBatchRepository interface {
	MarkOutboxPublishedBatch(context.Context, []OutboxRecord) error
}

type HotIndex interface {
	RepairRegister(context.Context, valkey.Entry) (bool, error)
	AcquireLease(context.Context, int, time.Duration) (string, bool, error)
	RenewLease(context.Context, int, string, time.Duration) error
	ReleaseLease(context.Context, int, string) error
	ClaimDue(context.Context, int, string, time.Duration, time.Duration, int) ([]valkey.ClaimedEntry, error)
	ReclaimExpired(context.Context, int, string, int) ([]valkey.Entry, error)
	Complete(context.Context, uuid.UUID, int64, string) (bool, error)
	Heartbeat(context.Context, int, string, string, string) (time.Time, error)
	BucketCount() int
}

// HotIndexBatchRegistrar is an optional optimized registration path.
// Results are aligned with entries and may report failures per bucket.
type HotIndexBatchRegistrar interface {
	RepairRegisterBatch(context.Context, []valkey.Entry) ([]valkey.RepairRegisterResult, error)
}

type ReadyRecord struct {
	DeliveryID       uuid.UUID
	ScheduleRevision int64
	DeliverAt        time.Time
	DestinationType  domain.DestinationType
}

type ReadyRepository interface {
	ResolveReady(context.Context, []valkey.ClaimedEntry) ([]ReadyRecord, error)
	MarkReadyPublished(context.Context, []ReadyRecord) error
}

type ReadyBatchPublisher interface {
	PublishReadyBatch(context.Context, []natsjs.PublishRequest) ([]natsjs.PublishRequest, error)
}

type RepairCursor struct {
	DeliverAt  time.Time
	DeliveryID uuid.UUID
}

type RepairRepository interface {
	RepairPage(context.Context, time.Duration, RepairCursor, int) ([]valkey.Entry, error)
}

type OverdueRepository interface {
	ReconcileOverdue(context.Context, time.Duration, int) (int, error)
}

type ProcessingReaperRepository interface {
	ReapExpiredProcessing(context.Context, time.Duration, int) (int, error)
}

type RetentionRepository interface {
	DeleteTerminal(context.Context, time.Duration, int) (int, int, error)
	DeletePublishedOutbox(context.Context, time.Duration, int) (int, error)
}

type Publisher interface {
	Publish(context.Context, natsjs.PublishRequest) error
}

type ErrorHandler func(component string, err error)

type LoopObserver func(component string, duration time.Duration, succeeded, fullBatch bool)

type PublishObserver func(kind natsjs.OutboxKind, duration time.Duration)

type BatchSizeObserver func(size int)

type ResultObserver func(result string)

type CountObserver func(count int)

type WakeLagObserver func(lag time.Duration)

type OperationErrorObserver func(operation string)

func report(handler ErrorHandler, component string, err error) {
	if handler != nil {
		handler(component, err)
	}
}

func observeLoop(
	observer LoopObserver,
	component string,
	started time.Time,
	succeeded bool,
	fullBatch bool,
) {
	if observer != nil {
		observer(component, time.Since(started), succeeded, fullBatch)
	}
}

type Backoff struct {
	Initial time.Duration
	Max     time.Duration
	mu      sync.Mutex
	rng     *rand.Rand
}

func NewBackoff(initial, max time.Duration, seed int64) *Backoff {
	return &Backoff{Initial: initial, Max: max, rng: rand.New(rand.NewSource(seed))}
}

func (b *Backoff) Delay(attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	exponent := math.Min(float64(attempt-1), 62)
	base := float64(b.Initial) * math.Pow(2, exponent)
	if base > float64(b.Max) {
		base = float64(b.Max)
	}
	b.mu.Lock()
	factor := 0.5 + b.rng.Float64()*0.5
	b.mu.Unlock()
	return time.Duration(base * factor)
}

func wait(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func shortError(err error) string {
	const limit = 1024
	text := err.Error()
	if len(text) > limit {
		return text[:limit]
	}
	return text
}
