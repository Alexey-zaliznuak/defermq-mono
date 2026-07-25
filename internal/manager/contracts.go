package manager

import (
	"context"
	"math"
	"math/rand"
	"sync"
	"time"

	"github.com/defermq/defermq/internal/domain"
	"github.com/defermq/defermq/internal/hotstorage/natsjs"
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
	ClaimOutbox(context.Context, string, int, time.Duration) ([]OutboxRecord, error)
	MarkOutboxPublished(context.Context, OutboxRecord) error
	MarkOutboxFailed(context.Context, OutboxRecord, time.Duration, string) error
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

func report(handler ErrorHandler, component string, err error) {
	if handler != nil {
		handler(component, err)
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
