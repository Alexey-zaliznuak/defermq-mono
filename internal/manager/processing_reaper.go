package manager

import (
	"context"
	"errors"
	"time"
)

type ProcessingReaperConfig struct {
	Interval      time.Duration
	RecoveryDelay time.Duration
	BatchSize     int
}

type ProcessingReaper struct {
	Repository ProcessingReaperRepository
	Config     ProcessingReaperConfig
	OnError    ErrorHandler
}

func (r *ProcessingReaper) Run(ctx context.Context) error {
	if r.Repository == nil || r.Config.Interval <= 0 || r.Config.RecoveryDelay < 0 || r.Config.BatchSize <= 0 {
		return errors.New("invalid processing reaper configuration")
	}
	for {
		count, err := r.Repository.ReapExpiredProcessing(ctx, r.Config.RecoveryDelay, r.Config.BatchSize)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			report(r.OnError, "processing_reaper", err)
		}
		if err == nil && count == r.Config.BatchSize {
			continue
		}
		if err := wait(ctx, r.Config.Interval); err != nil {
			return nil
		}
	}
}
