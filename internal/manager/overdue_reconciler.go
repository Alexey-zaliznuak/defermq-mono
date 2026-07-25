package manager

import (
	"context"
	"errors"
	"time"
)

type OverdueConfig struct {
	Interval  time.Duration
	Grace     time.Duration
	BatchSize int
}

type OverdueReconciler struct {
	Repository OverdueRepository
	Config     OverdueConfig
	OnError    ErrorHandler
	Observe    LoopObserver
}

func (r *OverdueReconciler) Run(ctx context.Context) error {
	if r.Repository == nil || r.Config.Interval <= 0 || r.Config.Grace < 0 || r.Config.BatchSize <= 0 {
		return errors.New("invalid overdue reconciler configuration")
	}
	for {
		started := time.Now()
		count, err := r.Repository.ReconcileOverdue(ctx, r.Config.Grace, r.Config.BatchSize)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			observeLoop(r.Observe, "overdue_reconciler", started, false, false)
			report(r.OnError, "overdue_reconciler", err)
		} else {
			observeLoop(r.Observe, "overdue_reconciler", started, true, count == r.Config.BatchSize)
		}
		if err := wait(ctx, r.Config.Interval); err != nil {
			return nil
		}
	}
}
