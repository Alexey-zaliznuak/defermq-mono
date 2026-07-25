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
}

func (r *OverdueReconciler) Run(ctx context.Context) error {
	if r.Repository == nil || r.Config.Interval <= 0 || r.Config.Grace < 0 || r.Config.BatchSize <= 0 {
		return errors.New("invalid overdue reconciler configuration")
	}
	for {
		_, err := r.Repository.ReconcileOverdue(ctx, r.Config.Grace, r.Config.BatchSize)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			report(r.OnError, "overdue_reconciler", err)
		}
		if err := wait(ctx, r.Config.Interval); err != nil {
			return nil
		}
	}
}
