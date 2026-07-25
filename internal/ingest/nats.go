package ingest

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

type StreamConfig struct {
	Name       string
	Subject    string
	Replicas   int
	MaxAge     time.Duration
	MaxBytes   int64
	MaxMsgSize int32
	Duplicates time.Duration
}

func (c StreamConfig) Validate() error {
	if c.Name == "" || c.Subject == "" || c.Replicas <= 0 || c.MaxAge <= 0 ||
		c.MaxBytes <= 0 || c.MaxMsgSize <= 0 || c.Duplicates <= 0 {
		return errors.New("invalid ingest stream configuration")
	}
	return nil
}

func EnsureStream(ctx context.Context, js jetstream.JetStream, cfg StreamConfig) (jetstream.Stream, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	streamSubject := StreamSubject(cfg.Subject)
	dlqSubject := DLQSubject(cfg.Subject)
	desired := jetstream.StreamConfig{
		Name:       cfg.Name,
		Subjects:   []string{streamSubject, dlqSubject},
		Retention:  jetstream.WorkQueuePolicy,
		Storage:    jetstream.FileStorage,
		Discard:    jetstream.DiscardNew,
		Replicas:   cfg.Replicas,
		MaxAge:     cfg.MaxAge,
		MaxBytes:   cfg.MaxBytes,
		MaxMsgSize: cfg.MaxMsgSize,
		Duplicates: cfg.Duplicates,
	}
	stream, err := js.Stream(ctx, cfg.Name)
	if errors.Is(err, jetstream.ErrStreamNotFound) {
		stream, err = js.CreateStream(ctx, desired)
		if err != nil {
			return nil, fmt.Errorf("create ingest stream %q: %w", cfg.Name, err)
		}
		return stream, nil
	}
	if err != nil {
		return nil, fmt.Errorf("load ingest stream %q: %w", cfg.Name, err)
	}
	info, err := stream.Info(ctx)
	if err != nil {
		return nil, fmt.Errorf("read ingest stream %q: %w", cfg.Name, err)
	}
	current := info.Config
	if current.Sealed || current.Storage != jetstream.FileStorage || current.NoAck ||
		(current.Retention != jetstream.LimitsPolicy && current.Retention != jetstream.WorkQueuePolicy) {
		return nil, fmt.Errorf("ingest stream %q has incompatible durability settings", cfg.Name)
	}
	changed := false
	if current.Retention == jetstream.LimitsPolicy {
		current.Retention = jetstream.WorkQueuePolicy
		changed = true
	}
	if slices.Contains(current.Subjects, cfg.Subject) && cfg.Subject != streamSubject {
		current.Subjects = slices.DeleteFunc(current.Subjects, func(subject string) bool {
			return subject == cfg.Subject
		})
		changed = true
	}
	if current.Discard != jetstream.DiscardNew {
		current.Discard = jetstream.DiscardNew
		changed = true
	}
	if !slices.Contains(current.Subjects, streamSubject) {
		current.Subjects = append(current.Subjects, streamSubject)
		changed = true
	}
	if !slices.Contains(current.Subjects, dlqSubject) {
		current.Subjects = append(current.Subjects, dlqSubject)
		changed = true
	}
	if current.Replicas < cfg.Replicas {
		current.Replicas = cfg.Replicas
		changed = true
	}
	if current.MaxAge > 0 && current.MaxAge < cfg.MaxAge {
		current.MaxAge = cfg.MaxAge
		changed = true
	}
	if current.MaxBytes >= 0 && current.MaxBytes < cfg.MaxBytes {
		current.MaxBytes = cfg.MaxBytes
		changed = true
	}
	if current.MaxMsgSize >= 0 && current.MaxMsgSize < cfg.MaxMsgSize {
		current.MaxMsgSize = cfg.MaxMsgSize
		changed = true
	}
	if current.Duplicates < cfg.Duplicates {
		current.Duplicates = cfg.Duplicates
		changed = true
	}
	if !changed {
		return stream, nil
	}
	stream, err = js.UpdateStream(ctx, current)
	if err != nil {
		return nil, fmt.Errorf("update ingest stream %q: %w", cfg.Name, err)
	}
	return stream, nil
}

// PrepareShardedConsumers prevents the legacy wildcard durable and the new
// sharded durables from processing the same stream concurrently. Deletion is
// deliberately opt-in because the caller must first stop every legacy manager.
func PrepareShardedConsumers(
	ctx context.Context,
	js jetstream.JetStream,
	streamName string,
	legacyDurable string,
	deleteLegacy bool,
) (uint64, error) {
	if js == nil || streamName == "" || legacyDurable == "" {
		return 0, errors.New("invalid ingest consumer migration configuration")
	}
	stream, err := js.Stream(ctx, streamName)
	if err != nil {
		return 0, fmt.Errorf("load ingest stream %q for consumer migration: %w", streamName, err)
	}
	consumer, err := stream.Consumer(ctx, legacyDurable)
	if errors.Is(err, jetstream.ErrConsumerNotFound) {
		return 0, nil
	} else if err != nil {
		return 0, fmt.Errorf("inspect legacy ingest consumer %q: %w", legacyDurable, err)
	}
	if !deleteLegacy {
		return 0, fmt.Errorf(
			"legacy ingest consumer %q still exists; stop legacy managers, then enable its explicit deletion",
			legacyDurable,
		)
	}
	info, err := consumer.Info(ctx)
	if err != nil {
		return 0, fmt.Errorf("read legacy ingest consumer %q: %w", legacyDurable, err)
	}
	startSequence := info.AckFloor.Stream + 1
	if err := stream.DeleteConsumer(ctx, legacyDurable); err != nil &&
		!errors.Is(err, jetstream.ErrConsumerNotFound) {
		return 0, fmt.Errorf("delete legacy ingest consumer %q: %w", legacyDurable, err)
	}
	return startSequence, nil
}

type asyncPublisher interface {
	PublishMsgAsync(*nats.Msg, ...jetstream.PublishOpt) (jetstream.PubAckFuture, error)
}

type publishItem struct {
	ctx     context.Context
	command Command
	result  chan publishResult
}

type publishResult struct {
	sequence uint64
	err      error
}

type BatchPublisher struct {
	js            asyncPublisher
	subjectPrefix string
	shardCount    int
	batchSize     int
	flushInterval time.Duration
	queue         chan publishItem
	startOnce     sync.Once
}

func NewBatchPublisher(js asyncPublisher, subject string, batchSize int, flushInterval time.Duration, capacity int, shards ...int) (*BatchPublisher, error) {
	if js == nil || subject == "" || batchSize <= 0 || flushInterval <= 0 || capacity < batchSize {
		return nil, errors.New("invalid ingest batch publisher configuration")
	}
	shardCount := DefaultShardCount
	if len(shards) > 0 {
		shardCount = shards[0]
	}
	if shardCount <= 0 {
		return nil, errors.New("ingest shard count must be positive")
	}
	return &BatchPublisher{
		js: js, subjectPrefix: subject, shardCount: shardCount,
		batchSize: batchSize, flushInterval: flushInterval,
		queue: make(chan publishItem, capacity),
	}, nil
}

func (p *BatchPublisher) Publish(ctx context.Context, command Command) error {
	_, err := p.PublishWithSequence(ctx, command)
	return err
}

func (p *BatchPublisher) PublishWithSequence(ctx context.Context, command Command) (uint64, error) {
	if err := command.Validate(); err != nil {
		return 0, err
	}
	item := publishItem{ctx: ctx, command: command, result: make(chan publishResult, 1)}
	select {
	case p.queue <- item:
	case <-ctx.Done():
		return 0, ctx.Err()
	}
	select {
	case result := <-item.result:
		return result.sequence, result.err
	case <-ctx.Done():
		return 0, ctx.Err()
	}
}

func (p *BatchPublisher) Run(ctx context.Context) error {
	var run bool
	p.startOnce.Do(func() { run = true })
	if !run {
		return errors.New("ingest batch publisher already started")
	}
	for {
		select {
		case <-ctx.Done():
			p.rejectQueued(ctx.Err())
			return nil
		case first := <-p.queue:
			batch := []publishItem{first}
			timer := time.NewTimer(p.flushInterval)
			collecting := true
			for collecting && len(batch) < p.batchSize {
				select {
				case item := <-p.queue:
					batch = append(batch, item)
				case <-timer.C:
					collecting = false
				case <-ctx.Done():
					timer.Stop()
					for _, item := range batch {
						item.result <- publishResult{err: ctx.Err()}
					}
					p.rejectQueued(ctx.Err())
					return nil
				}
			}
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			p.flush(batch)
		}
	}
}

func (p *BatchPublisher) flush(batch []publishItem) {
	futures := make([]jetstream.PubAckFuture, len(batch))
	for i, item := range batch {
		body, err := json.Marshal(item.command)
		if err == nil {
			msg := nats.NewMsg(ShardSubject(p.subjectPrefix, item.command.DeliveryID, p.shardCount))
			msg.Data = body
			futures[i], err = p.js.PublishMsgAsync(msg, jetstream.WithMsgID(MessageID(item.command)))
		}
		if err != nil {
			item.result <- publishResult{err: fmt.Errorf("publish ingest command: %w", err)}
		}
	}
	for i, future := range futures {
		if future == nil {
			continue
		}
		select {
		case ack := <-future.Ok():
			if ack == nil || ack.Stream == "" {
				batch[i].result <- publishResult{err: errors.New("ingest publish returned an empty PubAck")}
			} else {
				batch[i].result <- publishResult{sequence: ack.Sequence}
			}
		case err := <-future.Err():
			batch[i].result <- publishResult{err: fmt.Errorf("publish ingest command: %w", err)}
		case <-batch[i].ctx.Done():
			batch[i].result <- publishResult{err: batch[i].ctx.Err()}
		}
	}
}

func (p *BatchPublisher) rejectQueued(err error) {
	for {
		select {
		case item := <-p.queue:
			item.result <- publishResult{err: err}
		default:
			return
		}
	}
}
