package observability

import (
	"context"
	"errors"
	"fmt"
	"time"

	"go.uber.org/zap"
)

type OutboxGaugeSnapshot struct {
	Pending   float64
	OldestAge float64
	Locked    float64
}

type ManagerGaugeSnapshot struct {
	UnpromotedHeadroom        float64
	UnpromotedExists          bool
	OldestUnpromotedDeliverAt float64
	ScheduledDue              float64
	Processing                float64
	ProcessingExpired         float64
	Outbox                    map[string]OutboxGaugeSnapshot
}

type PusherGaugeSnapshot struct {
	ProcessingOwned     float64
	ProcessingOldestAge float64
	ConsumerPending     map[string]float64
	ConsumerAckPending  map[string]float64
}

type ManagerGaugeFetcher func(context.Context) (ManagerGaugeSnapshot, error)
type PusherGaugeFetcher func(context.Context) (PusherGaugeSnapshot, error)

func RunManagerGaugeCollector(
	ctx context.Context,
	interval time.Duration,
	queryTimeout time.Duration,
	logger *zap.Logger,
	metrics *ManagerMetrics,
	fetch ManagerGaugeFetcher,
) error {
	if metrics == nil || fetch == nil {
		return errors.New("manager metrics and fetcher are required")
	}
	if logger == nil {
		logger = zap.NewNop()
	}
	return runBackground(ctx, interval, func() {
		started := time.Now()
		snapshot, err := safeFetchManager(ctx, queryTimeout, fetch)
		metrics.CollectorDuration.Observe(time.Since(started).Seconds())
		if err != nil {
			metrics.CollectorSuccess.Set(0)
			metrics.CollectorErrors.Inc()
			logger.Warn("manager metrics collection failed", zap.Error(err))
			return
		}
		applyManagerSnapshot(metrics, snapshot)
		metrics.CollectorSuccess.Set(1)
		metrics.CollectorLastSuccess.Set(float64(time.Now().Unix()))
	})
}

func RunPusherGaugeCollector(
	ctx context.Context,
	interval time.Duration,
	queryTimeout time.Duration,
	logger *zap.Logger,
	metrics *PusherMetrics,
	fetch PusherGaugeFetcher,
) error {
	if metrics == nil || fetch == nil {
		return errors.New("pusher metrics and fetcher are required")
	}
	if logger == nil {
		logger = zap.NewNop()
	}
	return runBackground(ctx, interval, func() {
		started := time.Now()
		snapshot, err := safeFetchPusher(ctx, queryTimeout, fetch)
		metrics.CollectorDuration.Observe(time.Since(started).Seconds())
		if err != nil {
			metrics.CollectorSuccess.Set(0)
			metrics.CollectorErrors.Inc()
			logger.Warn("pusher metrics collection failed", zap.Error(err))
			return
		}
		applyPusherSnapshot(metrics, snapshot)
		metrics.CollectorSuccess.Set(1)
		metrics.CollectorLastSuccess.Set(float64(time.Now().Unix()))
	})
}

func runBackground(ctx context.Context, interval time.Duration, collect func()) error {
	if interval <= 0 {
		return errors.New("metrics collection interval must be positive")
	}
	collect()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			collect()
		}
	}
}

func safeFetchManager(ctx context.Context, timeout time.Duration, fetch ManagerGaugeFetcher) (snapshot ManagerGaugeSnapshot, err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("panic in manager metrics collector: %v", recovered)
		}
	}()
	queryCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	return fetch(queryCtx)
}

func safeFetchPusher(ctx context.Context, timeout time.Duration, fetch PusherGaugeFetcher) (snapshot PusherGaugeSnapshot, err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("panic in pusher metrics collector: %v", recovered)
		}
	}()
	queryCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	return fetch(queryCtx)
}

func applyManagerSnapshot(metrics *ManagerMetrics, snapshot ManagerGaugeSnapshot) {
	metrics.UnpromotedHeadroom.Set(snapshot.UnpromotedHeadroom)
	if snapshot.UnpromotedExists {
		metrics.UnpromotedExists.Set(1)
	} else {
		metrics.UnpromotedExists.Set(0)
	}
	metrics.OldestUnpromotedDeliverAt.Set(snapshot.OldestUnpromotedDeliverAt)
	metrics.ScheduledDue.Set(snapshot.ScheduledDue)
	metrics.Processing.Set(snapshot.Processing)
	metrics.ProcessingExpired.Set(snapshot.ProcessingExpired)
	for _, kind := range []string{"schedule", "ready"} {
		value := snapshot.Outbox[kind]
		metrics.OutboxPending.WithLabelValues(kind).Set(value.Pending)
		metrics.OutboxOldestAge.WithLabelValues(kind).Set(value.OldestAge)
		metrics.OutboxLocked.WithLabelValues(kind).Set(value.Locked)
	}
}

func applyPusherSnapshot(metrics *PusherMetrics, snapshot PusherGaugeSnapshot) {
	metrics.ProcessingOwned.Set(snapshot.ProcessingOwned)
	metrics.ProcessingOldestAge.Set(snapshot.ProcessingOldestAge)
	for _, destination := range []string{"http", "kafka", "rabbit", "postgres"} {
		metrics.NATSConsumerPending.WithLabelValues(destination).Set(snapshot.ConsumerPending[destination])
		metrics.NATSConsumerAckPending.WithLabelValues(destination).Set(snapshot.ConsumerAckPending[destination])
	}
}
