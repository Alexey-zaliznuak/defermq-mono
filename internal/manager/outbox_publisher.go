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
	Kind         natsjs.OutboxKind
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
	Observe    LoopObserver
	OnPublish  PublishObserver
}

func (w *OutboxWorker) Run(ctx context.Context) error {
	if w.Repository == nil || w.Publisher == nil || w.Backoff == nil || w.Config.WorkerID == "" ||
		w.Config.Kind != natsjs.OutboxReady ||
		w.Config.BatchSize <= 0 || w.Config.PollInterval <= 0 || w.Config.LockTTL <= 0 {
		return errors.New("invalid outbox worker configuration")
	}
	for {
		started := time.Now()
		records, err := w.Repository.ClaimOutbox(
			ctx, w.Config.WorkerID, w.Config.Kind, w.Config.BatchSize, w.Config.LockTTL,
		)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			observeLoop(w.Observe, "outbox", started, false, false)
			report(w.OnError, "outbox", fmt.Errorf("claim outbox: %w", err))
			if err := wait(ctx, w.Backoff.Delay(1)); err != nil {
				return nil
			}
			continue
		}
		if len(records) == 0 {
			observeLoop(w.Observe, "outbox", started, true, false)
			if err := wait(ctx, w.Config.PollInterval); err != nil {
				return nil
			}
			continue
		}
		succeeded := true
		if err := w.processBatch(ctx, records); err != nil {
			succeeded = false
			if ctx.Err() != nil {
				return nil
			}
		}
		observeLoop(w.Observe, "outbox", started, succeeded, len(records) == w.Config.BatchSize)
	}
}

func (w *OutboxWorker) processBatch(ctx context.Context, records []OutboxRecord) error {
	publisher, ok := w.Publisher.(ReadyBatchPublisher)
	if !ok {
		completed := make([]OutboxRecord, 0, len(records))
		var failures []error
		for _, record := range records {
			started := time.Now()
			err := w.Publisher.Publish(ctx, publishRequest(record))
			if w.OnPublish != nil {
				w.OnPublish(record.Kind, time.Since(started))
			}
			if err == nil {
				completed = append(completed, record)
				continue
			}
			delay := w.Backoff.Delay(record.PublishAttempts)
			if markErr := w.Repository.MarkOutboxFailed(
				ctx, record, delay, shortError(err),
			); markErr != nil {
				report(w.OnError, "outbox", fmt.Errorf(
					"mark publish failed: %w", errors.Join(err, markErr),
				))
				failures = append(failures, markErr)
				continue
			}
			report(w.OnError, "outbox", fmt.Errorf("publish outbox %d: %w", record.ID, err))
			failures = append(failures, err)
		}
		if err := w.completePublished(ctx, completed); err != nil {
			report(w.OnError, "outbox", fmt.Errorf("mark outbox batch published: %w", err))
			failures = append(failures, err)
		}
		return errors.Join(failures...)
	}

	requests := make([]natsjs.PublishRequest, len(records))
	type publishKey struct {
		deliveryID       string
		scheduleRevision int64
	}
	recordByKey := make(map[publishKey]OutboxRecord, len(records))
	for i, record := range records {
		requests[i] = publishRequest(record)
		recordByKey[publishKey{
			deliveryID:       record.DeliveryID.String(),
			scheduleRevision: record.ScheduleRevision,
		}] = record
	}

	started := time.Now()
	published, publishErr := publisher.PublishReadyBatch(ctx, requests)
	duration := time.Since(started)
	if w.OnPublish != nil {
		for range requests {
			w.OnPublish(natsjs.OutboxReady, duration)
		}
	}

	completed := make([]OutboxRecord, 0, len(published))
	completedKeys := make(map[publishKey]struct{}, len(published))
	var failures []error
	for _, request := range published {
		key := publishKey{
			deliveryID:       request.DeliveryID.String(),
			scheduleRevision: request.ScheduleRevision,
		}
		record, exists := recordByKey[key]
		if !exists || request.Kind != natsjs.OutboxReady {
			failures = append(failures, fmt.Errorf(
				"batch publisher returned unknown ready publication %s revision %d",
				request.DeliveryID, request.ScheduleRevision,
			))
			continue
		}
		if _, duplicate := completedKeys[key]; duplicate {
			continue
		}
		completedKeys[key] = struct{}{}
		completed = append(completed, record)
	}

	failureCause := publishErr
	if failureCause == nil && len(completed) != len(records) {
		failureCause = errors.New("ready batch publish was not acknowledged")
	}
	if failureCause != nil {
		report(w.OnError, "outbox", fmt.Errorf("publish ready batch: %w", failureCause))
		failures = append(failures, failureCause)
	}
	for key, record := range recordByKey {
		if _, success := completedKeys[key]; success {
			continue
		}
		delay := w.Backoff.Delay(record.PublishAttempts)
		if err := w.Repository.MarkOutboxFailed(
			ctx, record, delay, shortError(failureCause),
		); err != nil {
			report(w.OnError, "outbox", fmt.Errorf("mark publish failed: %w", err))
			failures = append(failures, err)
		}
	}
	if err := w.completePublished(ctx, completed); err != nil {
		report(w.OnError, "outbox", fmt.Errorf("mark outbox batch published: %w", err))
		failures = append(failures, err)
	}
	return errors.Join(failures...)
}

func (w *OutboxWorker) process(ctx context.Context, record OutboxRecord) error {
	started := time.Now()
	err := w.Publisher.Publish(ctx, publishRequest(record))
	if w.OnPublish != nil {
		w.OnPublish(record.Kind, time.Since(started))
	}
	if err != nil {
		delay := w.Backoff.Delay(record.PublishAttempts)
		if markErr := w.Repository.MarkOutboxFailed(ctx, record, delay, shortError(err)); markErr != nil {
			report(w.OnError, "outbox", fmt.Errorf("mark publish failed: %w", errors.Join(err, markErr)))
			return markErr
		}
		report(w.OnError, "outbox", fmt.Errorf("publish outbox %d: %w", record.ID, err))
		return err
	}
	if err := w.Repository.MarkOutboxPublished(ctx, record); err != nil {
		report(w.OnError, "outbox", fmt.Errorf("mark outbox published: %w", err))
		return err
	}
	return nil
}

func (w *OutboxWorker) completePublished(ctx context.Context, records []OutboxRecord) error {
	if len(records) == 0 {
		return nil
	}
	if batch, ok := w.Repository.(OutboxBatchRepository); ok {
		return batch.MarkOutboxPublishedBatch(ctx, records)
	}
	var failures []error
	for _, record := range records {
		if err := w.Repository.MarkOutboxPublished(ctx, record); err != nil {
			failures = append(failures, err)
		}
	}
	return errors.Join(failures...)
}

func publishRequest(record OutboxRecord) natsjs.PublishRequest {
	return natsjs.PublishRequest{
		Kind:             record.Kind,
		DeliveryID:       record.DeliveryID,
		ScheduleRevision: record.ScheduleRevision,
		DeliverAt:        record.DeliverAt,
		DestinationType:  record.DestinationType,
	}
}
