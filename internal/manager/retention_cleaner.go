package manager

import (
	"context"
	"errors"
	"time"
)

type RetentionConfig struct {
	Interval          time.Duration
	TerminalRetention time.Duration
	OutboxRetention   time.Duration
	BatchSize         int
}

type RetentionCleaner struct {
	Repository RetentionRepository
	Config     RetentionConfig
	OnError    ErrorHandler
}

func (c *RetentionCleaner) Run(ctx context.Context) error {
	if c.Repository == nil || c.Config.Interval <= 0 || c.Config.TerminalRetention <= 0 ||
		c.Config.OutboxRetention <= 0 || c.Config.BatchSize <= 0 {
		return errors.New("invalid retention cleaner configuration")
	}
	for {
		full, err := c.clean(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			report(c.OnError, "retention_cleaner", err)
		}
		if err == nil && full {
			continue
		}
		if err := wait(ctx, c.Config.Interval); err != nil {
			return nil
		}
	}
}

func (c *RetentionCleaner) clean(ctx context.Context) (bool, error) {
	deliveries, _, err := c.Repository.DeleteTerminal(ctx, c.Config.TerminalRetention, c.Config.BatchSize)
	if err != nil {
		return false, err
	}
	outbox, err := c.Repository.DeletePublishedOutbox(ctx, c.Config.OutboxRetention, c.Config.BatchSize)
	if err != nil {
		return false, err
	}
	return deliveries == c.Config.BatchSize || outbox == c.Config.BatchSize, nil
}
