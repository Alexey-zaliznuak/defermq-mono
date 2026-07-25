package postgres

import (
	"context"
	"fmt"
	"time"
)

type OutboxMetrics struct {
	Pending   int64
	Locked    int64
	OldestAge time.Duration
}

type ManagerDBMetrics struct {
	CollectedAt               time.Time
	OldestUnpromotedDeliverAt *time.Time
	UnpromotedHeadroom        time.Duration
	ScheduledDue              int64
	Processing                int64
	ProcessingExpired         int64
	Outbox                    map[OutboxKind]OutboxMetrics
}

type PusherDBMetrics struct {
	CollectedAt         time.Time
	ProcessingOwned     int64
	ProcessingOldestAge time.Duration
}

// CollectManagerDBMetrics performs bounded, exact prototype queries. It is
// intended for a background collector, never directly from /metrics.
func (s *Store) CollectManagerDBMetrics(ctx context.Context) (ManagerDBMetrics, error) {
	ctx, cancel := s.withTimeout(ctx)
	defer cancel()
	var result ManagerDBMetrics
	var oldest *time.Time
	var headroomSeconds *float64
	err := s.pool.QueryRow(ctx, `
		SELECT
			clock_timestamp(),
			min(deliver_at) FILTER (
				WHERE status = 'scheduled'
				  AND hot_registered_revision IS DISTINCT FROM schedule_revision
			),
			extract(epoch FROM (
				min(deliver_at) FILTER (
					WHERE status = 'scheduled'
					  AND hot_registered_revision IS DISTINCT FROM schedule_revision
				) - clock_timestamp()
			)),
			count(*) FILTER (
				WHERE status = 'scheduled' AND deliver_at <= clock_timestamp()
			),
			count(*) FILTER (WHERE status = 'processing'),
			count(*) FILTER (
				WHERE status = 'processing' AND processing_until < clock_timestamp()
			)
		FROM deliveries`).Scan(
		&result.CollectedAt, &oldest, &headroomSeconds, &result.ScheduledDue,
		&result.Processing, &result.ProcessingExpired)
	if err != nil {
		return ManagerDBMetrics{}, fmt.Errorf("collect delivery metrics: %w", err)
	}
	result.OldestUnpromotedDeliverAt = oldest
	if headroomSeconds != nil {
		result.UnpromotedHeadroom = time.Duration(*headroomSeconds * float64(time.Second))
	}
	result.Outbox = map[OutboxKind]OutboxMetrics{
		OutboxSchedule: {},
		OutboxReady:    {},
	}
	rows, err := s.pool.Query(ctx, `
		SELECT kind,
			count(*),
			count(*) FILTER (
				WHERE locked_until IS NOT NULL AND locked_until >= clock_timestamp()
			),
			coalesce(extract(epoch FROM (
				clock_timestamp() - min(created_at)
			)), 0)
		FROM nats_outbox
		WHERE published_at IS NULL
		GROUP BY kind`)
	if err != nil {
		return ManagerDBMetrics{}, fmt.Errorf("collect outbox metrics: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var kind OutboxKind
		var metrics OutboxMetrics
		var oldestAgeSeconds float64
		if err := rows.Scan(&kind, &metrics.Pending, &metrics.Locked, &oldestAgeSeconds); err != nil {
			return ManagerDBMetrics{}, fmt.Errorf("scan outbox metrics: %w", err)
		}
		metrics.OldestAge = time.Duration(oldestAgeSeconds * float64(time.Second))
		result.Outbox[kind] = metrics
	}
	if err := rows.Err(); err != nil {
		return ManagerDBMetrics{}, fmt.Errorf("iterate outbox metrics: %w", err)
	}
	return result, nil
}

func (s *Store) CollectPusherDBMetrics(ctx context.Context, owner string) (PusherDBMetrics, error) {
	ctx, cancel := s.withTimeout(ctx)
	defer cancel()
	var result PusherDBMetrics
	var oldestSeconds float64
	err := s.pool.QueryRow(ctx, `
		SELECT
			clock_timestamp(),
			count(*) FILTER (
				WHERE status = 'processing' AND ($1 = '' OR processing_owner = $1)
			),
			coalesce(max(extract(epoch FROM (
				clock_timestamp() - last_attempt_at
			))) FILTER (
				WHERE status = 'processing'
				  AND ($1 = '' OR processing_owner = $1)
			), 0)
		FROM deliveries`, owner).Scan(
		&result.CollectedAt, &result.ProcessingOwned, &oldestSeconds)
	if err != nil {
		return PusherDBMetrics{}, fmt.Errorf("collect pusher metrics: %w", err)
	}
	result.ProcessingOldestAge = time.Duration(oldestSeconds * float64(time.Second))
	return result, nil
}

type PoolStats struct {
	TotalConnections    int32
	IdleConnections     int32
	AcquiredConnections int32
	MaxConnections      int32
	AcquireCount        int64
	AcquireDuration     time.Duration
}

func (s *Store) PoolStats() PoolStats {
	stat := s.pool.Stat()
	return PoolStats{
		TotalConnections:    stat.TotalConns(),
		IdleConnections:     stat.IdleConns(),
		AcquiredConnections: stat.AcquiredConns(),
		MaxConnections:      stat.MaxConns(),
		AcquireCount:        stat.AcquireCount(),
		AcquireDuration:     stat.AcquireDuration(),
	}
}
