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
}

func (p *Promoter) Run(ctx context.Context) error {
	if p.Repository == nil || p.Config.HotHorizon <= 0 || p.Config.BatchSize <= 0 ||
		p.Config.PollInterval <= 0 || p.Config.ErrorBackoff <= 0 {
		return errors.New("invalid promoter configuration")
	}
	for {
		cutoff, err := p.Repository.PromotionCutoff(ctx, p.Config.HotHorizon)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			report(p.OnError, "promoter_cutoff", err)
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
				report(p.OnError, "promoter_batch", err)
				if err := wait(ctx, p.Config.ErrorBackoff); err != nil {
					return nil
				}
				break
			}
			if result.Candidates < 0 || result.Candidates > p.Config.BatchSize {
				return fmt.Errorf("repository returned invalid promoter candidate count %d", result.Candidates)
			}
			if result.Candidates == p.Config.BatchSize {
				continue
			}
			if err := wait(ctx, p.Config.PollInterval); err != nil {
				return nil
			}
			break
		}
	}
}
