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
		config.FetchMaxWait <= 0 || config.ProcessingLease <= 0 ||
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
	queue := make(chan Message, p.config.QueueSize)
	workCtx, cancelWork := context.WithCancel(context.Background())
	var workers sync.WaitGroup
	for worker := 0; worker < p.config.Workers; worker++ {
		workers.Add(1)
		go func(workerID int) {
			defer workers.Done()
			for message := range queue {
				p.handle(workCtx, workerID, message)
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
		batch := min(p.config.FetchBatchSize, capacity)
		messages, err := p.consumer.Fetch(ctx, batch, p.config.FetchMaxWait)
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
		for _, message := range messages {
			select {
			case queue <- message:
			case <-ctx.Done():
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

func (p *Pool) handle(parent context.Context, workerID int, message Message) {
	destinationType := string(p.consumer.Type())
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
		_ = message.Term(parent)
		return
	}
	fields := []zap.Field{
		zap.String("delivery_id", event.DeliveryID.String()),
		zap.Int64("schedule_revision", event.ScheduleRevision),
		zap.String("destination_type", string(event.DestinationType)),
		zap.Int("worker_id", workerID),
	}

	now := p.now().UTC()
	if wait := event.DeliverAt.Sub(now); wait > p.config.ClockSkewTolerance {
		if p.metrics != nil {
			p.metrics.EarlyEvents.WithLabelValues(destinationType).Inc()
		}
		p.logger.Debug("ready event arrived early", append(fields, zap.Duration("wait", wait))...)
		_ = message.Nak(parent, wait)
		return
	}
	claim, err := p.repository.Claim(
		parent,
		event.DeliveryID,
		event.ScheduleRevision,
		p.owner,
		p.config.ProcessingLease,
		p.config.ClockSkewTolerance,
	)
	if err != nil {
		if p.metrics != nil {
			p.metrics.Claims.WithLabelValues(destinationType, "error").Inc()
		}
		p.logger.Warn("delivery claim failed", append(fields, zap.Error(err))...)
		_ = message.Nak(parent, p.config.TransitionRetry)
		return
	}
	if claim.Reason != Claimed {
		if p.metrics != nil {
			p.metrics.Claims.WithLabelValues(destinationType, string(claim.Reason)).Inc()
		}
		if claim.Reason == ClaimTooEarly {
			wait := claim.Wait
			if wait <= 0 {
				wait = p.config.TransitionRetry
			}
			_ = message.Nak(parent, wait)
			return
		}
		_ = message.Ack(parent)
		return
	}
	if p.metrics != nil {
		p.metrics.Claims.WithLabelValues(destinationType, "claimed").Inc()
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

	payload, err := p.repository.LoadPayload(parent, deliveryRecord.PayloadID, p.config.MaxPayloadBytes)
	if err != nil {
		p.finishFailure(parent, message, deliveryRecord, delivery.NewPushError(
			"payload_load_failed",
			!errors.Is(err, domain.ErrNotFound) && !errors.Is(err, domain.ErrPayloadTooLarge),
			err,
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
