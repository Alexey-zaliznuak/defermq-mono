package manager

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/defermq/defermq/internal/hotstorage/valkey"
)

type RepairerConfig struct {
	HotHorizon time.Duration
	Interval   time.Duration
	BatchSize  int
}

type Repairer struct {
	Repository    RepairRepository
	Index         HotIndex
	Config        RepairerConfig
	OnError       ErrorHandler
	Observe       LoopObserver
	OnRegister    ResultObserver
	OnValkeyError OperationErrorObserver
}

func (r *Repairer) Run(ctx context.Context) error {
	if r.Repository == nil || r.Index == nil || r.Config.HotHorizon <= 0 ||
		r.Config.Interval <= 0 || r.Config.BatchSize <= 0 {
		return errors.New("invalid repairer configuration")
	}
	for {
		started := time.Now()
		cursor := RepairCursor{}
		succeeded := true
		full := false
		for {
			entries, err := r.Repository.RepairPage(ctx, r.Config.HotHorizon, cursor, r.Config.BatchSize)
			if err != nil {
				if ctx.Err() != nil {
					return nil
				}
				report(r.OnError, "repairer", fmt.Errorf("read repair page: %w", err))
				succeeded = false
				break
			}
			if err := r.registerPage(ctx, entries); err != nil {
				report(r.OnError, "repairer", fmt.Errorf("register repair page: %w", err))
				succeeded = false
			}
			if len(entries) > 0 {
				last := entries[len(entries)-1]
				cursor = RepairCursor{DeliverAt: last.DueAt, DeliveryID: last.DeliveryID}
			}
			if !succeeded || len(entries) < r.Config.BatchSize {
				break
			}
			full = true
		}
		observeLoop(r.Observe, "repairer", started, succeeded, full)
		if wait(ctx, r.Config.Interval) != nil {
			return nil
		}
	}
}

func (r *Repairer) registerPage(ctx context.Context, entries []valkey.Entry) error {
	if batch, ok := r.Index.(HotIndexBatchRegistrar); ok {
		results, batchErr := batch.RepairRegisterBatch(ctx, entries)
		if len(results) != len(entries) {
			err := fmt.Errorf(
				"repair-register batch returned %d results for %d entries",
				len(results), len(entries),
			)
			if batchErr != nil {
				err = errors.Join(err, batchErr)
			}
			for range entries {
				r.observeRegistration(false, err)
			}
			return err
		}
		var failures []error
		for _, result := range results {
			r.observeRegistration(result.Inserted, result.Err)
			if result.Err != nil {
				failures = append(failures, result.Err)
			}
		}
		if batchErr != nil {
			failures = append(failures, batchErr)
		}
		return errors.Join(failures...)
	}

	var failures []error
	for _, entry := range entries {
		inserted, err := r.Index.RepairRegister(ctx, entry)
		r.observeRegistration(inserted, err)
		if err != nil {
			failures = append(failures, err)
			break
		}
	}
	return errors.Join(failures...)
}

func (r *Repairer) observeRegistration(inserted bool, err error) {
	if err != nil {
		if r.OnRegister != nil {
			r.OnRegister("error")
		}
		if r.OnValkeyError != nil {
			r.OnValkeyError("repair_register")
		}
		return
	}
	if r.OnRegister != nil {
		result := "existing"
		if inserted {
			result = "inserted"
		}
		r.OnRegister(result)
	}
}
