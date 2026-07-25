package manager

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/defermq/defermq/internal/hotstorage/natsjs"
	"github.com/defermq/defermq/internal/hotstorage/valkey"
)

type RegistrarConfig struct {
	WorkerID     string
	BatchSize    int
	PollInterval time.Duration
	LockTTL      time.Duration
}

type Registrar struct {
	Repository    OutboxRepository
	Index         HotIndex
	Backoff       *Backoff
	Config        RegistrarConfig
	OnError       ErrorHandler
	Observe       LoopObserver
	OnBatch       BatchSizeObserver
	OnZADD        ResultObserver
	OnValkeyError OperationErrorObserver
}

func (r *Registrar) Run(ctx context.Context) error {
	if r.Repository == nil || r.Index == nil || r.Backoff == nil || r.Config.WorkerID == "" ||
		r.Config.BatchSize <= 0 || r.Config.PollInterval <= 0 || r.Config.LockTTL <= 0 {
		return errors.New("invalid registrar configuration")
	}
	for {
		started := time.Now()
		records, err := r.Repository.ClaimOutbox(
			ctx, r.Config.WorkerID, natsjs.OutboxHotRegister, r.Config.BatchSize, r.Config.LockTTL,
		)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			observeLoop(r.Observe, "registrar", started, false, false)
			report(r.OnError, "registrar", fmt.Errorf("claim hot-register outbox: %w", err))
			if wait(ctx, r.Backoff.Delay(1)) != nil {
				return nil
			}
			continue
		}
		if r.OnBatch != nil {
			r.OnBatch(len(records))
		}
		succeeded := true
		if err := r.registerBatch(ctx, records); err != nil {
			succeeded = false
			if ctx.Err() != nil {
				return nil
			}
		}
		observeLoop(r.Observe, "registrar", started, succeeded, len(records) == r.Config.BatchSize)
		if len(records) == 0 && wait(ctx, r.Config.PollInterval) != nil {
			return nil
		}
	}
}

func (r *Registrar) register(ctx context.Context, record OutboxRecord) error {
	if err := r.registerIndex(ctx, record); err != nil {
		return err
	}
	if err := r.completePublished(ctx, []OutboxRecord{record}); err != nil {
		report(r.OnError, "registrar", err)
		return err
	}
	return nil
}

func (r *Registrar) registerBatch(ctx context.Context, records []OutboxRecord) error {
	batch, ok := r.Index.(HotIndexBatchRegistrar)
	if ok {
		return r.registerIndexBatch(ctx, batch, records)
	}

	completed := make([]OutboxRecord, 0, len(records))
	var failures []error
	for _, record := range records {
		if err := r.registerIndex(ctx, record); err != nil {
			failures = append(failures, err)
			continue
		}
		completed = append(completed, record)
	}
	if err := r.completePublished(ctx, completed); err != nil {
		report(r.OnError, "registrar", err)
		failures = append(failures, err)
	}
	return errors.Join(failures...)
}

func (r *Registrar) registerIndexBatch(
	ctx context.Context,
	index HotIndexBatchRegistrar,
	records []OutboxRecord,
) error {
	entries := make([]valkey.Entry, len(records))
	for i, record := range records {
		entries[i] = valkey.Entry{
			DeliveryID: record.DeliveryID,
			Revision:   record.ScheduleRevision,
			DueAt:      record.DeliverAt,
		}
	}
	results, batchErr := index.RepairRegisterBatch(ctx, entries)
	if len(results) != len(records) {
		err := fmt.Errorf(
			"repair-register batch returned %d results for %d records: %w",
			len(results), len(records), batchErr,
		)
		if batchErr == nil {
			err = fmt.Errorf(
				"repair-register batch returned %d results for %d records",
				len(results), len(records),
			)
		}
		var failures []error
		for _, record := range records {
			failures = append(failures, r.failRegistration(ctx, record, err))
		}
		return errors.Join(failures...)
	}

	completed := make([]OutboxRecord, 0, len(records))
	var failures []error
	for i, result := range results {
		if result.Err != nil {
			failures = append(failures, r.failRegistration(ctx, records[i], result.Err))
			continue
		}
		r.observeRegistration(result.Inserted)
		completed = append(completed, records[i])
	}
	if batchErr != nil {
		failures = append(failures, batchErr)
	}
	if err := r.completePublished(ctx, completed); err != nil {
		report(r.OnError, "registrar", err)
		failures = append(failures, err)
	}
	return errors.Join(failures...)
}

func (r *Registrar) registerIndex(ctx context.Context, record OutboxRecord) error {
	inserted, err := r.Index.RepairRegister(ctx, valkey.Entry{
		DeliveryID: record.DeliveryID,
		Revision:   record.ScheduleRevision,
		DueAt:      record.DeliverAt,
	})
	if err != nil {
		return r.failRegistration(ctx, record, err)
	}
	r.observeRegistration(inserted)
	return nil
}

func (r *Registrar) observeRegistration(inserted bool) {
	if r.OnZADD != nil {
		result := "existing"
		if inserted {
			result = "inserted"
		}
		r.OnZADD(result)
	}
}

func (r *Registrar) failRegistration(
	ctx context.Context,
	record OutboxRecord,
	registerErr error,
) error {
	if r.OnZADD != nil {
		r.OnZADD("error")
	}
	if r.OnValkeyError != nil {
		r.OnValkeyError("repair_register")
	}
	delay := r.Backoff.Delay(record.PublishAttempts)
	if markErr := r.Repository.MarkOutboxFailed(
		ctx, record, delay, shortError(registerErr),
	); markErr != nil {
		joined := errors.Join(registerErr, markErr)
		report(r.OnError, "registrar", joined)
		return joined
	}
	report(r.OnError, "registrar", registerErr)
	return registerErr
}

func (r *Registrar) completePublished(ctx context.Context, records []OutboxRecord) error {
	if len(records) == 0 {
		return nil
	}
	if batch, ok := r.Repository.(OutboxBatchRepository); ok {
		return batch.MarkOutboxPublishedBatch(ctx, records)
	}
	var failures []error
	for _, record := range records {
		if err := r.Repository.MarkOutboxPublished(ctx, record); err != nil {
			failures = append(failures, err)
		}
	}
	return errors.Join(failures...)
}
