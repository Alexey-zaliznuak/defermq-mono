package manager

import (
	"context"
	"errors"
	"fmt"
	"time"
)

type PromoterConfig struct {
	HotHorizon   time.Duration
	BatchSize    int
	PollInterval time.Duration
	ErrorBackoff time.Duration
}

type Promoter struct {
	Repository PromoterRepository
	Config     PromoterConfig
	OnError    ErrorHandler
	Observe    LoopObserver
}

func (p *Promoter) Run(ctx context.Context) error {
	if p.Repository == nil || p.Config.HotHorizon <= 0 || p.Config.BatchSize <= 0 ||
		p.Config.PollInterval <= 0 || p.Config.ErrorBackoff <= 0 {
		return errors.New("invalid promoter configuration")
	}
	for {
		started := time.Now()
		fullBatch := false
		cutoff, err := p.Repository.PromotionCutoff(ctx, p.Config.HotHorizon)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			observeLoop(p.Observe, "promoter", started, false, false)
			report(p.OnError, "promoter", fmt.Errorf("get promotion cutoff: %w", err))
			if err := wait(ctx, p.Config.ErrorBackoff); err != nil {
				return nil
			}
			continue
		}
		for {
			result, err := p.Repository.PromoteBatch(ctx, cutoff, p.Config.BatchSize)
			if err != nil {
				if ctx.Err() != nil {
					return nil
				}
				observeLoop(p.Observe, "promoter", started, false, fullBatch)
				report(p.OnError, "promoter", fmt.Errorf("promote batch: %w", err))
				if err := wait(ctx, p.Config.ErrorBackoff); err != nil {
					return nil
				}
				break
			}
			if result.Candidates < 0 || result.Candidates > p.Config.BatchSize {
				err := fmt.Errorf("repository returned invalid promoter candidate count %d", result.Candidates)
				observeLoop(p.Observe, "promoter", started, false, fullBatch)
				report(p.OnError, "promoter", err)
				return err
			}
			if result.Candidates == p.Config.BatchSize {
				fullBatch = true
				continue
			}
			observeLoop(p.Observe, "promoter", started, true, fullBatch)
			if err := wait(ctx, p.Config.PollInterval); err != nil {
				return nil
			}
			break
		}
	}
}
