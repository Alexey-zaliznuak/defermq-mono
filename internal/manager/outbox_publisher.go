package manager

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/defermq/defermq/internal/hotstorage/natsjs"
)

type OutboxWorkerConfig struct {
	WorkerID     string
	BatchSize    int
	PollInterval time.Duration
	LockTTL      time.Duration
}

type OutboxWorker struct {
	Repository OutboxRepository
	Publisher  Publisher
	Backoff    *Backoff
	Config     OutboxWorkerConfig
	OnError    ErrorHandler
}

func (w *OutboxWorker) Run(ctx context.Context) error {
	if w.Repository == nil || w.Publisher == nil || w.Backoff == nil || w.Config.WorkerID == "" ||
		w.Config.BatchSize <= 0 || w.Config.PollInterval <= 0 || w.Config.LockTTL <= 0 {
		return errors.New("invalid outbox worker configuration")
	}
	for {
		records, err := w.Repository.ClaimOutbox(ctx, w.Config.WorkerID, w.Config.BatchSize, w.Config.LockTTL)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			report(w.OnError, "outbox_claim", err)
			if err := wait(ctx, w.Backoff.Delay(1)); err != nil {
				return nil
			}
			continue
		}
		if len(records) == 0 {
			if err := wait(ctx, w.Config.PollInterval); err != nil {
				return nil
			}
			continue
		}
		for _, record := range records {
			if err := w.process(ctx, record); err != nil && ctx.Err() != nil {
				return nil
			}
		}
	}
}

func (w *OutboxWorker) process(ctx context.Context, record OutboxRecord) error {
	err := w.Publisher.Publish(ctx, natsjs.PublishRequest{
		Kind:             record.Kind,
		DeliveryID:       record.DeliveryID,
		ScheduleRevision: record.ScheduleRevision,
		DeliverAt:        record.DeliverAt,
		DestinationType:  record.DestinationType,
	})
	if err != nil {
		delay := w.Backoff.Delay(record.PublishAttempts)
		if markErr := w.Repository.MarkOutboxFailed(ctx, record, delay, shortError(err)); markErr != nil {
			report(w.OnError, "outbox_mark_failed", errors.Join(err, markErr))
			return markErr
		}
		report(w.OnError, "outbox_publish", fmt.Errorf("outbox %d: %w", record.ID, err))
		return err
	}
	if err := w.Repository.MarkOutboxPublished(ctx, record); err != nil {
		report(w.OnError, "outbox_mark_published", err)
		return err
	}
	return nil
}
