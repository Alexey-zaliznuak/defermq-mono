package ingest

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

type BatchStore interface {
	ApplyIngestBatch(context.Context, []Command) error
}

type dlqPublisher interface {
	Publish(context.Context, string, []byte, ...jetstream.PublishOpt) (*jetstream.PubAck, error)
}

type WriterObserver interface {
	ObserveIngestBatch(size int)
	ObserveIngestCommit(rows int, duration time.Duration, result string)
	ObserveIngestRedelivery()
	ObserveIngestDLQ(result string)
}

type WriterConfig struct {
	Stream         string
	Subject        string
	Durable        string
	FilterSubjects []string
	ShardCount     int
	WorkerCount    int
	WorkerIndex    int
	StartSequence  uint64
	BatchSize      int
	FlushInterval  time.Duration
	AckWait        time.Duration
	MaxAckPending  int
	MaxDeliver     int
}

type Writer struct {
	store           BatchStore
	consumer        jetstream.Consumer
	js              dlqPublisher
	config          WriterConfig
	onError         func(error)
	observer        WriterObserver
	onCycle         func(bool)
	fetch           func(context.Context, int) (jetstream.MessageBatch, error)
	productionFetch func(context.Context, int, time.Duration) (jetstream.MessageBatch, error)
}

type writerMessage interface {
	Data() []byte
	Metadata() (*jetstream.MsgMetadata, error)
	DoubleAck(context.Context) error
	Nak() error
	Term() error
}

func NewWriter(ctx context.Context, js jetstream.JetStream, store BatchStore, config WriterConfig, onError func(error)) (*Writer, error) {
	if js == nil || store == nil || config.Stream == "" || config.Subject == "" || config.Durable == "" ||
		len(config.FilterSubjects) == 0 || config.ShardCount <= 0 || config.WorkerCount <= 0 ||
		config.WorkerIndex < 0 || config.WorkerIndex >= config.WorkerCount ||
		config.BatchSize <= 0 || config.FlushInterval <= 0 || config.AckWait <= 0 ||
		config.MaxAckPending < config.BatchSize || config.MaxDeliver <= 0 {
		return nil, errors.New("invalid ingest writer configuration")
	}
	existing, err := js.Consumer(ctx, config.Stream, config.Durable)
	if err == nil {
		info, infoErr := existing.Info(ctx)
		if infoErr != nil {
			return nil, fmt.Errorf("read ingest consumer %q: %w", config.Durable, infoErr)
		}
		if info.Config.DeliverPolicy == jetstream.DeliverByStartSequencePolicy {
			config.StartSequence = info.Config.OptStartSeq
		}
	} else if !errors.Is(err, jetstream.ErrConsumerNotFound) {
		return nil, fmt.Errorf("inspect ingest consumer %q: %w", config.Durable, err)
	}
	consumerConfig := config.consumerConfig()
	consumer, err := js.CreateOrUpdateConsumer(ctx, config.Stream, consumerConfig)
	if err != nil {
		return nil, fmt.Errorf("ensure ingest consumer %q: %w", config.Durable, err)
	}
	return &Writer{store: store, consumer: consumer, js: js, config: config, onError: onError}, nil
}

func (c WriterConfig) consumerConfig() jetstream.ConsumerConfig {
	config := jetstream.ConsumerConfig{
		Name: c.Durable, Durable: c.Durable,
		DeliverPolicy: jetstream.DeliverAllPolicy, AckPolicy: jetstream.AckExplicitPolicy,
		AckWait: c.AckWait, MaxDeliver: c.MaxDeliver,
		FilterSubjects: append([]string(nil), c.FilterSubjects...), ReplayPolicy: jetstream.ReplayInstantPolicy,
		MaxAckPending: c.MaxAckPending, MaxRequestBatch: c.BatchSize,
		MaxRequestExpires: c.FlushInterval,
	}
	if c.StartSequence > 0 {
		config.DeliverPolicy = jetstream.DeliverByStartSequencePolicy
		config.OptStartSeq = c.StartSequence
	}
	return config
}

// ShardWriterConfigs assigns every shard to exactly one writer. A delivery is
// always published to the same shard, so all of its commands remain on one
// consumer and are applied in JetStream sequence order.
func ShardWriterConfigs(base WriterConfig) ([]WriterConfig, error) {
	if base.ShardCount <= 0 || base.WorkerCount <= 0 || base.WorkerCount > base.ShardCount ||
		base.Subject == "" || base.Durable == "" {
		return nil, errors.New("invalid ingest writer shard assignment")
	}
	configs := make([]WriterConfig, base.WorkerCount)
	for worker := range configs {
		configs[worker] = base
		configs[worker].WorkerIndex = worker
		configs[worker].Durable = fmt.Sprintf("%s-%d", base.Durable, worker+1)
		configs[worker].FilterSubjects = nil
	}
	for shard := 0; shard < base.ShardCount; shard++ {
		worker := shard % base.WorkerCount
		configs[worker].FilterSubjects = append(
			configs[worker].FilterSubjects,
			fmt.Sprintf("%s.%d", strings.TrimSuffix(base.Subject, ".*"), shard),
		)
	}
	return configs, nil
}

func (w *Writer) SetObserver(observer WriterObserver, onCycle func(bool)) {
	w.observer = observer
	w.onCycle = onCycle
}

func (w *Writer) Run(ctx context.Context) error {
	for ctx.Err() == nil {
		messages, ok := w.accumulate(ctx)
		if !ok {
			if ctx.Err() != nil {
				break
			}
			w.observeCycle(false)
			continue
		}
		if len(messages) == 0 {
			continue
		}
		w.observeCycle(w.processBatch(ctx, messages))
	}
	return nil
}

func (w *Writer) accumulate(ctx context.Context) ([]writerMessage, bool) {
	messages := make([]writerMessage, 0, w.config.BatchSize)
	var flushDeadline time.Time

	for len(messages) < w.config.BatchSize {
		fetchDeadline := flushDeadline
		if fetchDeadline.IsZero() {
			fetchDeadline = time.Now().Add(w.config.FlushInterval)
		}
		remaining := time.Until(fetchDeadline)
		if remaining <= 0 {
			return messages, true
		}

		// Production pulls expire on the server. Only fake fetchers use a
		// deadline context so tests can deterministically model that expiry.
		fetchCtx := ctx
		cancel := func() {}
		if w.fetch != nil {
			fetchCtx, cancel = context.WithDeadline(ctx, fetchDeadline)
		}
		messageCountBeforeFetch := len(messages)
		batch, err := w.fetchBatch(fetchCtx, w.config.BatchSize-len(messages), remaining)
		if err != nil {
			cancel()
			if ctx.Err() != nil {
				w.nakMessages(messages)
				return nil, false
			}
			w.report(err)
			w.nakMessages(messages)
			return nil, false
		}

		messageCh := batch.Messages()
		for message := range messageCh {
			if flushDeadline.IsZero() {
				flushDeadline = time.Now().Add(w.config.FlushInterval)
			}
			messages = append(messages, message)
		}
		cancel()

		if len(messages) == w.config.BatchSize {
			return messages, true
		}
		if ctx.Err() != nil {
			w.nakMessages(messages)
			return nil, false
		}
		if err := batch.Error(); err != nil && !errors.Is(err, context.DeadlineExceeded) &&
			!errors.Is(err, context.Canceled) {
			w.report(err)
			w.nakMessages(messages)
			return nil, false
		}
		if !flushDeadline.IsZero() && !time.Now().Before(flushDeadline) {
			return messages, true
		}

		// A normal Fetch waits for its context or returns messages. If a server
		// ends an empty pull early, wait for this pull's deadline to avoid a
		// tight retry loop.
		if len(messages) == messageCountBeforeFetch {
			if !waitUntil(ctx, fetchDeadline) {
				w.nakMessages(messages)
				return nil, false
			}
			if !flushDeadline.IsZero() {
				return messages, true
			}
		}
	}
	return messages, true
}

func (w *Writer) fetchBatch(
	ctx context.Context,
	size int,
	maxWait time.Duration,
) (jetstream.MessageBatch, error) {
	if w.fetch != nil {
		return w.fetch(ctx, size)
	}
	if w.productionFetch != nil {
		return w.productionFetch(ctx, size, maxWait)
	}
	// FetchNoWait only reserves messages that are already available, so the
	// accumulator can safely drain the returned channel without either
	// abandoning ack-pending messages or blocking after a server pull expires.
	// The accumulator itself owns the absolute flush window.
	return w.consumer.FetchNoWait(size)
}

func (w *Writer) nakMessages(messages []writerMessage) {
	for _, message := range messages {
		if err := message.Nak(); err != nil {
			w.report(err)
		}
	}
}

func waitUntil(ctx context.Context, deadline time.Time) bool {
	timer := time.NewTimer(time.Until(deadline))
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func (w *Writer) processBatch(ctx context.Context, messages []writerMessage) bool {
	if w.observer != nil {
		w.observer.ObserveIngestBatch(len(messages))
	}
	commands := make([]Command, 0, len(messages))
	valid := make([]writerMessage, 0, len(messages))
	for _, message := range messages {
		if metadata, err := message.Metadata(); err == nil && metadata.NumDelivered > 1 {
			if w.observer != nil {
				w.observer.ObserveIngestRedelivery()
			}
		}
		var command Command
		decodeErr := json.Unmarshal(message.Data(), &command)
		if decodeErr == nil {
			decodeErr = command.Validate()
		}
		if decodeErr != nil {
			w.poison(ctx, message, decodeErr)
			continue
		}
		commands = append(commands, command)
		valid = append(valid, message)
	}
	if len(commands) == 0 {
		return true
	}
	started := time.Now()
	if err := w.store.ApplyIngestBatch(ctx, commands); err != nil {
		if w.observer != nil {
			w.observer.ObserveIngestCommit(len(commands), time.Since(started), "error")
		}
		w.report(err)
		for _, message := range valid {
			metadata, metadataErr := message.Metadata()
			if metadataErr != nil {
				w.report(metadataErr)
			}
			if w.config.MaxDeliver > 0 && metadata != nil &&
				metadata.NumDelivered >= uint64(w.config.MaxDeliver) {
				w.poison(ctx, message, err)
				continue
			}
			if err := message.Nak(); err != nil {
				w.report(err)
			}
		}
		return false
	}
	if w.observer != nil {
		w.observer.ObserveIngestCommit(len(commands), time.Since(started), "success")
	}
	succeeded := true
	for _, message := range valid {
		ackCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), w.config.AckWait)
		err := message.DoubleAck(ackCtx)
		cancel()
		if err != nil {
			w.report(err)
			succeeded = false
		}
	}
	return succeeded
}

type poisonMessage struct {
	Error        string `json:"error"`
	Data         []byte `json:"data"`
	NumDelivered uint64 `json:"num_delivered"`
}

func (w *Writer) poison(ctx context.Context, message writerMessage, cause error) {
	metadata, _ := message.Metadata()
	var deliveries uint64
	if metadata != nil {
		deliveries = metadata.NumDelivered
	}
	body, err := json.Marshal(poisonMessage{
		Error: cause.Error(), Data: append([]byte(nil), message.Data()...),
		NumDelivered: deliveries,
	})
	if err == nil && w.js != nil {
		sum := sha256.Sum256(message.Data())
		_, err = w.js.Publish(ctx, DLQSubject(w.config.Subject), body,
			jetstream.WithMsgID(fmt.Sprintf("ingest-dlq-%x", sum[:16])))
	}
	if err != nil {
		if w.observer != nil {
			w.observer.ObserveIngestDLQ("error")
		}
		w.report(fmt.Errorf("publish ingest DLQ: %w", err))
		if nakErr := message.Nak(); nakErr != nil {
			w.report(nakErr)
		}
		return
	}
	if w.observer != nil {
		w.observer.ObserveIngestDLQ("success")
	}
	if err := message.Term(); err != nil {
		w.report(err)
	}
}

func (w *Writer) observeCycle(succeeded bool) {
	if w.onCycle != nil {
		w.onCycle(succeeded)
	}
}

func (w *Writer) report(err error) {
	if w.onError != nil && err != nil {
		w.onError(err)
	}
}
