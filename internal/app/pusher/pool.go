package pusher

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"time"

	"github.com/defermq/defermq/internal/delivery"
	"github.com/defermq/defermq/internal/domain"
	"github.com/defermq/defermq/internal/hotstorage/natsjs"
	"github.com/defermq/defermq/internal/observability"
	"github.com/google/uuid"
	"go.uber.org/zap"
)

type PoolConfig struct {
	Workers            int
	QueueSize          int
	FetchBatchSize     int
	FetchMaxWait       time.Duration
	ClaimBatchSize     int
	ClaimFlushInterval time.Duration
	ProcessingLease    time.Duration
	HeartbeatInterval  time.Duration
	ClockSkewTolerance time.Duration
	MaxPayloadBytes    int64
	HotHorizon         time.Duration
	TransitionRetry    time.Duration
	ShutdownTimeout    time.Duration
}

type Pool struct {
	config     PoolConfig
	owner      string
	consumer   Consumer
	repository Repository
	dispatcher *delivery.Dispatcher
	backoff    delivery.Backoff
	logger     *zap.Logger
	metrics    *observability.PusherMetrics
	now        func() time.Time
}

func NewPool(
	config PoolConfig,
	owner string,
	consumer Consumer,
	repository Repository,
	dispatcher *delivery.Dispatcher,
	backoff delivery.Backoff,
	logger *zap.Logger,
) (*Pool, error) {
	if config.Workers <= 0 || config.QueueSize < config.Workers || config.FetchBatchSize <= 0 ||
		config.FetchMaxWait <= 0 || config.ClaimBatchSize <= 0 || config.ClaimFlushInterval <= 0 ||
		config.ProcessingLease <= 0 ||
		config.HeartbeatInterval <= 0 || config.HeartbeatInterval >= config.ProcessingLease ||
		config.ClockSkewTolerance < 0 || config.MaxPayloadBytes <= 0 ||
		config.HotHorizon < 0 || config.TransitionRetry <= 0 || config.ShutdownTimeout <= 0 || owner == "" ||
		consumer == nil || repository == nil || dispatcher == nil {
		return nil, errors.New("invalid Pusher pool configuration")
	}
	if err := backoff.Validate(); err != nil {
		return nil, fmt.Errorf("retry backoff: %w", err)
	}
	if logger == nil {
		logger = zap.NewNop()
	}
	return &Pool{
		config:     config,
		owner:      owner,
		consumer:   consumer,
		repository: repository,
		dispatcher: dispatcher,
		backoff:    backoff,
		logger:     logger,
		now:        time.Now,
	}, nil
}

func (p *Pool) SetMetrics(metrics *observability.PusherMetrics) {
	p.metrics = metrics
}

func (p *Pool) Run(ctx context.Context) error {
	queue := make(chan claimedJob, p.config.QueueSize)
	workCtx, cancelWork := context.WithCancel(context.Background())
	var workers sync.WaitGroup
	for worker := 0; worker < p.config.Workers; worker++ {
		workers.Add(1)
		go func(workerID int) {
			defer workers.Done()
			for job := range queue {
				p.handleClaimed(workCtx, workerID, job)
			}
		}(worker)
	}

fetchLoop:
	for {
		if ctx.Err() != nil {
			break fetchLoop
		}
		capacity := cap(queue) - len(queue)
		if capacity == 0 {
			if !waitContext(ctx, 10*time.Millisecond) {
				break fetchLoop
			}
			continue
		}
		message, err := p.consumer.Next(ctx)
		if err != nil {
			if ctx.Err() != nil {
				break fetchLoop
			}
			p.logger.Warn("ready fetch failed", zap.String("destination_type", string(p.consumer.Type())), zap.Error(err))
			if !waitContext(ctx, 500*time.Millisecond) {
				break fetchLoop
			}
			continue
		}
		if message == nil {
			p.logger.Warn("ready consumer returned an empty message")
			if !waitContext(ctx, 100*time.Millisecond) {
				break fetchLoop
			}
			continue
		}
		limit := min(p.config.FetchBatchSize, p.config.ClaimBatchSize, capacity)
		messages := []Message{message}
		flushDeadline := time.Now().Add(p.config.ClaimFlushInterval)
		for len(messages) < limit {
			remaining := time.Until(flushDeadline)
			if remaining <= 0 {
				break
			}
			fetchCtx, cancelFetch := context.WithTimeout(ctx, remaining)
			next, nextErr := p.consumer.Next(fetchCtx)
			fetchTimedOut := errors.Is(fetchCtx.Err(), context.DeadlineExceeded)
			cancelFetch()
			if nextErr != nil {
				if ctx.Err() != nil {
					for _, unhandled := range messages {
						_ = unhandled.Nak(workCtx, p.config.TransitionRetry)
					}
					break fetchLoop
				}
				if errors.Is(nextErr, context.DeadlineExceeded) || fetchTimedOut {
					break
				}
				p.logger.Warn("ready microbatch fetch failed",
					zap.String("destination_type", string(p.consumer.Type())), zap.Error(nextErr))
				break
			}
			if next == nil {
				break
			}
			messages = append(messages, next)
		}
		jobs := p.claimMessages(ctx, workCtx, messages)
		for index, job := range jobs {
			select {
			case queue <- job:
			case <-ctx.Done():
				_ = job.message.Nak(workCtx, p.config.TransitionRetry)
				for _, unhandled := range jobs[index+1:] {
					_ = unhandled.message.Nak(workCtx, p.config.TransitionRetry)
				}
				break fetchLoop
			}
		}
	}
	close(queue)
	done := make(chan struct{})
	go func() {
		workers.Wait()
		close(done)
	}()
	timer := time.NewTimer(p.config.ShutdownTimeout)
	defer timer.Stop()
	select {
	case <-done:
	case <-timer.C:
		cancelWork()
		<-done
	}
	cancelWork()
	return nil
}

type pendingClaim struct {
	message Message
	event   natsjs.ReadyEvent
}

type claimedJob struct {
	message Message
	event   natsjs.ReadyEvent
	claim   ClaimResult
}

func (p *Pool) claimMessages(claimCtx, dispositionCtx context.Context, messages []Message) []claimedJob {
	destinationType := string(p.consumer.Type())
	pending := make([]pendingClaim, 0, len(messages))
	requests := make([]ClaimRequest, 0, len(messages))
	for _, message := range messages {
		if p.metrics != nil {
			p.metrics.MessagesReceived.WithLabelValues(destinationType).Inc()
		}
		event, err := decodeEvent(message.Data(), p.consumer.Type())
		if err != nil {
			if p.metrics != nil {
				p.metrics.EventsInvalid.WithLabelValues("validation").Inc()
				p.metrics.Acks.WithLabelValues(destinationType, "term").Inc()
			}
			p.logger.Warn("discarding invalid ready event", zap.Error(err))
			_ = message.Term(dispositionCtx)
			continue
		}
		now := p.now().UTC()
		if wait := event.DeliverAt.Sub(now); wait > p.config.ClockSkewTolerance {
			if p.metrics != nil {
				p.metrics.EarlyEvents.WithLabelValues(destinationType).Inc()
			}
			_ = message.Nak(dispositionCtx, wait)
			continue
		}
		pending = append(pending, pendingClaim{message: message, event: event})
		requests = append(requests, ClaimRequest{
			DeliveryID: event.DeliveryID, ScheduleRevision: event.ScheduleRevision,
		})
	}
	if len(requests) == 0 {
		return nil
	}
	claims, err := p.repository.ClaimBatch(
		claimCtx, requests, p.owner, p.config.ProcessingLease, p.config.ClockSkewTolerance,
	)
	if err != nil || len(claims) != len(pending) {
		if p.metrics != nil {
			p.metrics.Claims.WithLabelValues(destinationType, "error").Add(float64(len(pending)))
		}
		p.logger.Warn("delivery batch claim failed",
			zap.Int("batch_size", len(pending)), zap.Int("result_size", len(claims)), zap.Error(err))
		for _, item := range pending {
			_ = item.message.Nak(dispositionCtx, p.config.TransitionRetry)
		}
		return nil
	}
	jobs := make([]claimedJob, 0, len(claims))
	for index, claim := range claims {
		if p.metrics != nil {
			p.metrics.Claims.WithLabelValues(destinationType, string(claim.Reason)).Inc()
		}
		switch claim.Reason {
		case Claimed:
			jobs = append(jobs, claimedJob{
				message: pending[index].message, event: pending[index].event, claim: claim,
			})
		case ClaimTooEarly:
			wait := claim.Wait
			if wait <= 0 {
				wait = p.config.TransitionRetry
			}
			_ = pending[index].message.Nak(dispositionCtx, wait)
		case ClaimNotFound, ClaimStale, ClaimTerminal, ClaimProcessing:
			_ = pending[index].message.Ack(dispositionCtx)
		default:
			p.logger.Error("unknown batch claim outcome", zap.String("reason", string(claim.Reason)))
			_ = pending[index].message.Nak(dispositionCtx, p.config.TransitionRetry)
		}
	}
	return jobs
}

func (p *Pool) handleClaimed(parent context.Context, workerID int, job claimedJob) {
	message, event, claim := job.message, job.event, job.claim
	destinationType := string(p.consumer.Type())
	fields := []zap.Field{
		zap.String("delivery_id", event.DeliveryID.String()),
		zap.Int64("schedule_revision", event.ScheduleRevision),
		zap.String("destination_type", string(event.DestinationType)),
		zap.Int("worker_id", workerID),
	}
	now := p.now().UTC()
	if p.metrics != nil {
		p.metrics.Inflight.WithLabelValues(destinationType).Inc()
		defer p.metrics.Inflight.WithLabelValues(destinationType).Dec()
		p.metrics.DeliveryStartLag.WithLabelValues(destinationType).Observe(max(0, now.Sub(event.DeliverAt).Seconds()))
	}
	deliveryRecord := claim.Delivery
	if deliveryRecord == nil {
		p.logger.Error("claim returned no delivery", fields...)
		_ = message.Nak(parent, p.config.TransitionRetry)
		return
	}

	payload := claim.Payload
	if payload == nil {
		p.logger.Error("claim returned no payload", fields...)
		_ = message.Nak(parent, p.config.TransitionRetry)
		return
	}
	if payload.SizeBytes > p.config.MaxPayloadBytes {
		p.finishFailure(parent, message, deliveryRecord, delivery.NewPushError(
			"payload_load_failed",
			false,
			fmt.Errorf("%w: payload is %d bytes, limit is %d",
				domain.ErrPayloadTooLarge, payload.SizeBytes, p.config.MaxPayloadBytes),
		), fields)
		return
	}
	var destination domain.Destination
	if err := json.Unmarshal(deliveryRecord.Destination, &destination); err != nil {
		p.finishFailure(parent, message, deliveryRecord, delivery.NewPushError("invalid_destination", false, err), fields)
		return
	}
	if destination.Type != deliveryRecord.DestinationType || destination.Type != event.DestinationType {
		p.finishFailure(parent, message, deliveryRecord, delivery.NewPushError(
			"destination_type_mismatch",
			false,
			fmt.Errorf("stored destination type %q does not match delivery type %q", destination.Type, deliveryRecord.DestinationType),
		), fields)
		return
	}

	attemptCtx, cancel := context.WithCancel(parent)
	var ownershipLost atomic.Bool
	heartbeatDone := make(chan struct{})
	go p.heartbeat(attemptCtx, cancel, deliveryRecord.ID, &ownershipLost, heartbeatDone)
	attemptStarted := time.Now()
	pushErr := p.dispatcher.Push(attemptCtx, delivery.PushRequest{
		DeliveryID:       deliveryRecord.ID,
		ScheduleRevision: deliveryRecord.ScheduleRevision,
		Attempt:          deliveryRecord.Attempts,
		ScheduledAt:      deliveryRecord.DeliverAt,
		Destination:      destination,
		Payload:          payload.Body,
		ContentType:      payload.ContentType,
		Headers:          payload.Headers,
	})
	cancel()
	<-heartbeatDone
	if p.metrics != nil {
		p.metrics.AttemptDuration.WithLabelValues(destinationType).Observe(time.Since(attemptStarted).Seconds())
		info := delivery.ErrorInfo(pushErr)
		result := "success"
		retryable := false
		if pushErr != nil {
			result = "failure"
			retryable = info.Retryable
		}
		p.metrics.Attempts.WithLabelValues(destinationType, result, fmt.Sprintf("%t", retryable)).Inc()
	}
	if ownershipLost.Load() {
		p.logger.Warn("processing ownership lost during push", fields...)
		_ = message.Nak(parent, p.config.TransitionRetry)
		return
	}
	if pushErr != nil {
		p.finishFailure(parent, message, deliveryRecord, pushErr, fields)
		return
	}
	updated, err := p.repository.MarkDelivered(parent, deliveryRecord.ID, p.owner)
	if err != nil || !updated {
		p.logger.Warn("failed to commit successful delivery", append(fields, zap.Error(err), zap.Bool("updated", updated))...)
		_ = message.Nak(parent, p.config.TransitionRetry)
		return
	}
	if err := message.Ack(parent); err != nil {
		p.logger.Warn("ready ACK failed after delivery commit", append(fields, zap.Error(err))...)
	}
	if p.metrics != nil {
		p.metrics.DeliveryCompletionLag.WithLabelValues(destinationType).Observe(max(0, time.Since(event.DeliverAt).Seconds()))
		p.metrics.Acks.WithLabelValues(destinationType, "success").Inc()
	}
}

func (p *Pool) finishFailure(
	ctx context.Context,
	message Message,
	record *domain.Delivery,
	err error,
	fields []zap.Field,
) {
	info := delivery.ErrorInfo(err)
	errorText := delivery.ErrorMessage(info, 2048)
	var updated bool
	var transitionErr error
	if info.Retryable && record.Attempts < record.MaxAttempts {
		delay := p.backoff.Delay(record.Attempts, info.RetryAfter)
		updated, transitionErr = p.repository.ScheduleRetry(
			ctx, record.ID, p.owner, delay, errorText, p.config.HotHorizon,
		)
	} else {
		updated, transitionErr = p.repository.MarkDead(ctx, record.ID, p.owner, errorText)
	}
	if transitionErr != nil || !updated {
		p.logger.Warn("failed to commit delivery failure", append(fields,
			zap.Error(transitionErr),
			zap.Bool("updated", updated),
			zap.Bool("retryable", info.Retryable),
			zap.String("code", info.Code),
		)...)
		_ = message.Nak(ctx, p.config.TransitionRetry)
		return
	}
	if ackErr := message.Ack(ctx); ackErr != nil {
		p.logger.Warn("ready ACK failed after failure commit", append(fields, zap.Error(ackErr))...)
	}
	if p.metrics != nil {
		destinationType := string(record.DestinationType)
		if info.Retryable && record.Attempts < record.MaxAttempts {
			p.metrics.RetriesScheduled.WithLabelValues(destinationType).Inc()
		} else {
			p.metrics.Dead.WithLabelValues(destinationType, info.Code).Inc()
		}
		p.metrics.Acks.WithLabelValues(destinationType, "success").Inc()
	}
}

func (p *Pool) heartbeat(
	ctx context.Context,
	cancel context.CancelFunc,
	deliveryID uuid.UUID,
	lost *atomic.Bool,
	done chan<- struct{},
) {
	defer close(done)
	ticker := time.NewTicker(p.config.HeartbeatInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			updated, err := p.repository.Heartbeat(ctx, deliveryID, p.owner, p.config.ProcessingLease)
			if err != nil {
				if p.metrics != nil {
					p.metrics.ProcessingHeartbeat.WithLabelValues("error").Inc()
				}
				p.logger.Warn("processing heartbeat failed", zap.String("delivery_id", deliveryID.String()), zap.Error(err))
				continue
			}
			if !updated {
				if p.metrics != nil {
					p.metrics.ProcessingHeartbeat.WithLabelValues("ownership_lost").Inc()
				}
				lost.Store(true)
				cancel()
				return
			}
			if p.metrics != nil {
				p.metrics.ProcessingHeartbeat.WithLabelValues("success").Inc()
			}
		}
	}
}

func decodeEvent(data []byte, expected domain.DestinationType) (natsjs.ReadyEvent, error) {
	var event natsjs.ReadyEvent
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&event); err != nil {
		return event, fmt.Errorf("decode ready event: %w", err)
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return event, errors.New("ready event contains trailing data")
	}
	if err := event.Validate(expected); err != nil {
		return event, err
	}
	return event, nil
}

func waitContext(ctx context.Context, duration time.Duration) bool {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-timer.C:
		return true
	case <-ctx.Done():
		return false
	}
}
