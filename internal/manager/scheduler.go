package manager

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/defermq/defermq/internal/hotstorage/natsjs"
)

type SchedulerConfig struct {
	Owner          string
	Worker         int
	Workers        int
	PollInterval   time.Duration
	LeaseTTL       time.Duration
	InflightTTL    time.Duration
	PublishTimeout time.Duration
	EarlyWindow    time.Duration
	BatchSize      int
	ReclaimBatch   int
	ErrorBackoff   time.Duration
}

type Scheduler struct {
	Index         HotIndex
	Repository    ReadyRepository
	Publisher     ReadyBatchPublisher
	Config        SchedulerConfig
	OnError       ErrorHandler
	Observe       LoopObserver
	OnClaimed     CountObserver
	OnPublished   ResultObserver
	OnReclaimed   CountObserver
	OnWakeLag     WakeLagObserver
	OnValkeyError OperationErrorObserver
}

func (s *Scheduler) Run(ctx context.Context) error {
	if s.Index == nil || s.Repository == nil || s.Publisher == nil || s.Config.Owner == "" ||
		s.Config.Workers <= 0 || s.Config.Worker < 0 || s.Config.Worker >= s.Config.Workers ||
		s.Config.PollInterval <= 0 || s.Config.LeaseTTL <= 0 || s.Config.InflightTTL <= 0 ||
		s.Config.PublishTimeout <= 0 || s.Config.InflightTTL <= s.Config.PublishTimeout ||
		s.Config.EarlyWindow < 0 || s.Config.BatchSize <= 0 || s.Config.ReclaimBatch <= 0 ||
		s.Config.ErrorBackoff <= 0 {
		return errors.New("invalid scheduler configuration")
	}
	bucket := s.Config.Worker
	for {
		if bucket >= s.Index.BucketCount() {
			bucket = s.Config.Worker
		}
		started := time.Now()
		err := s.runBucket(ctx, bucket)
		if err != nil && ctx.Err() == nil {
			report(s.OnError, "scheduler", fmt.Errorf("bucket %d: %w", bucket, err))
		}
		observeLoop(s.Observe, "scheduler", started, err == nil, false)
		bucket += s.Config.Workers
		delay := s.Config.PollInterval
		if err != nil {
			delay = s.Config.ErrorBackoff
		}
		if wait(ctx, delay) != nil {
			return nil
		}
	}
}

func (s *Scheduler) runBucket(ctx context.Context, bucket int) error {
	token, acquired, err := s.Index.AcquireLease(ctx, bucket, s.Config.LeaseTTL)
	if err != nil {
		s.valkeyError("acquire_lease")
		return err
	}
	if !acquired {
		return err
	}
	defer func() {
		if err := s.Index.ReleaseLease(context.WithoutCancel(ctx), bucket, token); err != nil {
			s.valkeyError("release_lease")
		}
	}()
	if _, err := s.Index.Heartbeat(ctx, bucket, token, s.Config.Owner, "running"); err != nil {
		s.valkeyError("heartbeat")
		return err
	}
	reclaimed, err := s.Index.ReclaimExpired(ctx, bucket, token, s.Config.ReclaimBatch)
	if err != nil {
		s.valkeyError("reclaim_expired")
		return err
	}
	if s.OnReclaimed != nil {
		s.OnReclaimed(len(reclaimed))
	}
	claimed, err := s.Index.ClaimDue(
		ctx, bucket, token, s.Config.EarlyWindow, s.Config.InflightTTL, s.Config.BatchSize,
	)
	if err != nil {
		s.valkeyError("claim_due")
		return err
	}
	if s.OnClaimed != nil {
		s.OnClaimed(len(claimed))
	}
	if s.OnWakeLag != nil {
		now := time.Now()
		for _, entry := range claimed {
			lag := now.Sub(entry.DueAt)
			if lag < 0 {
				lag = 0
			}
			s.OnWakeLag(lag)
		}
	}
	if len(claimed) == 0 {
		return nil
	}
	records, err := s.Repository.ResolveReady(ctx, claimed)
	if err != nil {
		return err
	}
	current := make(map[string]struct{}, len(records))
	for _, record := range records {
		current[readyKey(record.DeliveryID.String(), record.ScheduleRevision)] = struct{}{}
	}
	for _, entry := range claimed {
		if _, ok := current[readyKey(entry.DeliveryID.String(), entry.Revision)]; !ok {
			if _, err := s.Index.Complete(ctx, entry.DeliveryID, entry.Revision, token); err != nil {
				s.valkeyError("complete")
				return err
			}
		}
	}
	if len(records) == 0 {
		return nil
	}
	if err := s.Index.RenewLease(ctx, bucket, token, s.Config.LeaseTTL); err != nil {
		s.valkeyError("renew_lease")
		return err
	}
	requests := make([]natsjs.PublishRequest, len(records))
	for i, record := range records {
		requests[i] = natsjs.PublishRequest{
			Kind: natsjs.OutboxReady, DeliveryID: record.DeliveryID,
			ScheduleRevision: record.ScheduleRevision, DeliverAt: record.DeliverAt,
			DestinationType: record.DestinationType,
		}
	}
	publishCtx, cancel := context.WithTimeout(ctx, s.Config.PublishTimeout)
	published, publishErr := s.Publisher.PublishReadyBatch(publishCtx, requests)
	cancel()
	successful, ackErr := acknowledgedReady(records, published)
	publishErr = errors.Join(publishErr, ackErr)
	if s.OnPublished != nil {
		for range successful {
			s.OnPublished("success")
		}
		for i := len(successful); i < len(requests); i++ {
			s.OnPublished("error")
		}
	}
	if len(successful) != 0 {
		if err := s.Repository.MarkReadyPublished(ctx, successful); err != nil {
			return err
		}
		if err := s.Index.RenewLease(ctx, bucket, token, s.Config.LeaseTTL); err != nil {
			s.valkeyError("renew_lease")
			return err
		}
		for _, record := range successful {
			if _, err := s.Index.Complete(ctx, record.DeliveryID, record.ScheduleRevision, token); err != nil {
				s.valkeyError("complete")
				return err
			}
		}
	}
	return publishErr
}

func (s *Scheduler) valkeyError(operation string) {
	if s.OnValkeyError != nil {
		s.OnValkeyError(operation)
	}
}

func readyKey(id string, revision int64) string {
	return fmt.Sprintf("%s:%d", id, revision)
}

func acknowledgedReady(
	records []ReadyRecord,
	published []natsjs.PublishRequest,
) ([]ReadyRecord, error) {
	expected := make(map[string]ReadyRecord, len(records))
	for _, record := range records {
		expected[readyKey(record.DeliveryID.String(), record.ScheduleRevision)] = record
	}
	successful := make([]ReadyRecord, 0, len(published))
	seen := make(map[string]struct{}, len(published))
	var failures []error
	for _, request := range published {
		key := readyKey(request.DeliveryID.String(), request.ScheduleRevision)
		record, ok := expected[key]
		if !ok || request.Kind != natsjs.OutboxReady {
			failures = append(failures, fmt.Errorf(
				"publisher acknowledged unknown ready delivery %s revision %d",
				request.DeliveryID, request.ScheduleRevision,
			))
			continue
		}
		if _, duplicate := seen[key]; duplicate {
			failures = append(failures, fmt.Errorf(
				"publisher acknowledged ready delivery %s revision %d more than once",
				request.DeliveryID, request.ScheduleRevision,
			))
			continue
		}
		seen[key] = struct{}{}
		successful = append(successful, record)
	}
	if len(successful) != len(records) {
		failures = append(failures, fmt.Errorf(
			"ready batch acknowledged %d of %d publications", len(successful), len(records),
		))
	}
	return successful, errors.Join(failures...)
}
