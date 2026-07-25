package ingest

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func TestWriterAccumulatesImmediatePartialFetches(t *testing.T) {
	store := &recordingStore{}
	ctx, cancel := context.WithCancel(context.Background())
	store.onApply = cancel
	messages := encodedMessages(t, store, 4)
	fetcher := &scriptedFetcher{steps: []fetchStep{
		{messages: messages[:2]},
		{messages: messages[2:]},
	}}
	writer := testRunWriter(store, fetcher, 4, time.Second)

	if err := writer.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if store.calls != 1 || len(store.rows) != 1 || store.rows[0] != 4 || fetcher.calls != 2 {
		t.Fatalf("calls=%d rows=%v fetches=%d", store.calls, store.rows, fetcher.calls)
	}
	for _, message := range messages {
		if !message.acked || message.nacked {
			t.Fatalf("ack=%v nak=%v", message.acked, message.nacked)
		}
	}
}

func TestWriterFlushDeadlineCommitsPartialBatch(t *testing.T) {
	store := &recordingStore{}
	ctx, cancel := context.WithCancel(context.Background())
	store.onApply = cancel
	messages := encodedMessages(t, store, 2)
	fetcher := &scriptedFetcher{steps: []fetchStep{
		{messages: messages},
		{waitForContext: true},
	}}
	writer := testRunWriter(store, fetcher, 4, 20*time.Millisecond)

	if err := writer.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if store.calls != 1 || len(store.rows) != 1 || store.rows[0] != 2 || fetcher.calls != 2 {
		t.Fatalf("calls=%d rows=%v fetches=%d", store.calls, store.rows, fetcher.calls)
	}
}

func TestWriterCommitsFullBatchWithoutAnotherFetch(t *testing.T) {
	store := &recordingStore{}
	ctx, cancel := context.WithCancel(context.Background())
	store.onApply = cancel
	messages := encodedMessages(t, store, 3)
	fetcher := &scriptedFetcher{steps: []fetchStep{{messages: messages}}}
	writer := testRunWriter(store, fetcher, 3, time.Hour)

	if err := writer.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if store.calls != 1 || store.rows[0] != 3 || fetcher.calls != 1 {
		t.Fatalf("calls=%d rows=%v fetches=%d", store.calls, store.rows, fetcher.calls)
	}
}

func TestWriterProductionFetchUsesParentContextAndServerMaxWait(t *testing.T) {
	type contextKey struct{}

	const flushInterval = time.Second
	parent := context.WithValue(context.Background(), contextKey{}, "parent")
	message := encodedMessage(t)
	var (
		fetchCtx context.Context
		maxWait  time.Duration
	)
	writer := &Writer{
		config: WriterConfig{
			BatchSize:     1,
			FlushInterval: flushInterval,
		},
		productionFetch: func(
			ctx context.Context,
			size int,
			wait time.Duration,
		) (jetstream.MessageBatch, error) {
			if size != 1 {
				t.Fatalf("fetch size=%d", size)
			}
			fetchCtx = ctx
			maxWait = wait
			batch := &scriptedMessageBatch{messages: make(chan jetstream.Msg, 1)}
			batch.messages <- message
			close(batch.messages)
			return batch, nil
		},
	}

	messages, ok := writer.accumulate(parent)
	if !ok || len(messages) != 1 {
		t.Fatalf("ok=%v messages=%d", ok, len(messages))
	}
	if fetchCtx.Value(contextKey{}) != "parent" {
		t.Fatal("production fetch did not receive parent context")
	}
	if deadline, hasDeadline := fetchCtx.Deadline(); hasDeadline {
		t.Fatalf("production fetch received normal flush deadline %v", deadline)
	}
	if maxWait <= 0 || maxWait > flushInterval ||
		maxWait < flushInterval-100*time.Millisecond {
		t.Fatalf("FetchMaxWait=%v, flush interval=%v", maxWait, flushInterval)
	}
}

func TestWriterAccumulatedCommitFailureNAKs(t *testing.T) {
	store := &recordingStore{err: errors.New("database unavailable")}
	ctx, cancel := context.WithCancel(context.Background())
	store.onApply = cancel
	messages := encodedMessages(t, store, 4)
	fetcher := &scriptedFetcher{steps: []fetchStep{
		{messages: messages[:2]},
		{messages: messages[2:]},
	}}
	writer := testRunWriter(store, fetcher, 4, time.Second)

	if err := writer.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if store.calls != 1 || fetcher.calls != 2 {
		t.Fatalf("calls=%d fetches=%d", store.calls, fetcher.calls)
	}
	for _, message := range messages {
		if message.acked || !message.nacked {
			t.Fatalf("ack=%v nak=%v", message.acked, message.nacked)
		}
	}
}

func TestWriterCancellationNAKsAccumulatedMessages(t *testing.T) {
	store := &recordingStore{}
	ctx, cancel := context.WithCancel(context.Background())
	messages := encodedMessages(t, store, 2)
	fetcher := &scriptedFetcher{steps: []fetchStep{
		{messages: messages},
		{waitForContext: true, onFetch: cancel},
	}}
	writer := testRunWriter(store, fetcher, 4, time.Second)

	if err := writer.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if store.calls != 0 || fetcher.calls != 2 {
		t.Fatalf("calls=%d fetches=%d", store.calls, fetcher.calls)
	}
	for _, message := range messages {
		if message.acked || !message.nacked {
			t.Fatalf("ack=%v nak=%v", message.acked, message.nacked)
		}
	}
}

func TestWriterACKsOnlyAfterCommit(t *testing.T) {
	store := &recordingStore{}
	message := encodedMessage(t)
	message.store = store
	message.numDelivered = 2
	observer := &recordingWriterObserver{}
	writer := &Writer{store: store, config: WriterConfig{AckWait: time.Second}, observer: observer}
	writer.processBatch(context.Background(), []writerMessage{message})
	if store.calls != 1 || !message.acked || message.nacked || message.ackBeforeCommit {
		t.Fatalf("calls=%d ack=%v nak=%v early=%v", store.calls, message.acked, message.nacked, message.ackBeforeCommit)
	}
	if observer.batchSize != 1 || observer.commitRows != 1 || observer.commitResult != "success" ||
		observer.redeliveries != 1 {
		t.Fatalf("unexpected observations: %+v", observer)
	}
}

func TestWriterNAKsOnCommitFailure(t *testing.T) {
	store := &recordingStore{err: errors.New("database unavailable")}
	message := encodedMessage(t)
	message.store = store
	writer := &Writer{store: store, config: WriterConfig{AckWait: time.Second}}
	writer.processBatch(context.Background(), []writerMessage{message})
	if message.acked || !message.nacked {
		t.Fatalf("ack=%v nak=%v", message.acked, message.nacked)
	}
}

func TestWriterTermsMalformedCommand(t *testing.T) {
	store := &recordingStore{}
	message := &recordingMessage{data: []byte(`{"kind":"broken"}`), store: store}
	writer := &Writer{store: store, config: WriterConfig{AckWait: time.Second}}
	writer.processBatch(context.Background(), []writerMessage{message})
	if !message.termed || store.calls != 0 {
		t.Fatalf("term=%v store calls=%d", message.termed, store.calls)
	}
}

func TestWriterPublishesDLQAtMaxDelivery(t *testing.T) {
	store := &recordingStore{err: errors.New("database unavailable")}
	message := encodedMessage(t)
	message.store = store
	message.numDelivered = 3
	dlq := &recordingDLQPublisher{}
	observer := &recordingWriterObserver{}
	writer := &Writer{
		store: store, js: dlq,
		config:   WriterConfig{AckWait: time.Second, MaxDeliver: 3, Subject: "defermq.ingest.commands"},
		observer: observer,
	}
	writer.processBatch(context.Background(), []writerMessage{message})
	if !message.termed || message.nacked || message.acked {
		t.Fatalf("term=%v nak=%v ack=%v", message.termed, message.nacked, message.acked)
	}
	if dlq.subject != DLQSubject("defermq.ingest.commands") || len(dlq.payload) == 0 {
		t.Fatalf("DLQ subject=%q payload=%d", dlq.subject, len(dlq.payload))
	}
	if observer.commitResult != "error" || observer.dlqResult != "success" {
		t.Fatalf("unexpected observations: %+v", observer)
	}
}

func TestShardWriterConfigsAssignAllShardsExactlyOnce(t *testing.T) {
	configs, err := ShardWriterConfigs(WriterConfig{
		Subject: "defermq.ingest.commands", Durable: "defermq-ingest-writer",
		ShardCount: 32, WorkerCount: 8,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(configs) != 8 {
		t.Fatalf("writers=%d", len(configs))
	}
	seen := make(map[string]int, 32)
	for worker, config := range configs {
		if config.WorkerIndex != worker ||
			config.Durable != fmt.Sprintf("defermq-ingest-writer-%d", worker+1) {
			t.Fatalf("worker %d has index=%d durable=%q", worker, config.WorkerIndex, config.Durable)
		}
		for _, subject := range config.FilterSubjects {
			seen[subject]++
		}
	}
	for shard := 0; shard < 32; shard++ {
		subject := fmt.Sprintf("defermq.ingest.commands.%d", shard)
		if seen[subject] != 1 {
			t.Fatalf("subject %q assigned %d times", subject, seen[subject])
		}
		expectedWorker := shard % 8
		found := false
		for _, subject := range configs[expectedWorker].FilterSubjects {
			if subject == fmt.Sprintf("defermq.ingest.commands.%d", shard) {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("shard %d not assigned to worker %d", shard, expectedWorker)
		}
	}
	if len(seen) != 32 {
		t.Fatalf("unique assigned subjects=%d", len(seen))
	}
}

func TestWriterConsumerConfigPreservesStreamOrdering(t *testing.T) {
	writer := WriterConfig{
		Durable: "defermq-ingest-writer-1",
		FilterSubjects: []string{
			"defermq.ingest.commands.0",
			"defermq.ingest.commands.8",
		},
		BatchSize: 100, FlushInterval: time.Second, AckWait: 30 * time.Second,
		MaxAckPending: 1000, MaxDeliver: 20,
	}
	config := writer.consumerConfig()
	if config.DeliverPolicy != jetstream.DeliverAllPolicy ||
		config.AckPolicy != jetstream.AckExplicitPolicy ||
		config.ReplayPolicy != jetstream.ReplayInstantPolicy {
		t.Fatalf("unexpected ordering policies: %+v", config)
	}
	if config.FilterSubject != "" || len(config.FilterSubjects) != 2 {
		t.Fatalf("unexpected subject filters: %+v", config)
	}
	if config.MaxRequestBatch != writer.BatchSize ||
		config.MaxRequestExpires != writer.FlushInterval {
		t.Fatalf("unexpected pull ordering config: %+v", config)
	}
	writer.StartSequence = 41
	config = writer.consumerConfig()
	if config.DeliverPolicy != jetstream.DeliverByStartSequencePolicy ||
		config.OptStartSeq != writer.StartSequence {
		t.Fatalf("migration did not preserve legacy ack floor: %+v", config)
	}
}

type recordingWriterObserver struct {
	batchSize    int
	commitRows   int
	commitResult string
	redeliveries int
	dlqResult    string
}

func (o *recordingWriterObserver) ObserveIngestBatch(size int) { o.batchSize = size }
func (o *recordingWriterObserver) ObserveIngestCommit(rows int, _ time.Duration, result string) {
	o.commitRows, o.commitResult = rows, result
}
func (o *recordingWriterObserver) ObserveIngestRedelivery() { o.redeliveries++ }
func (o *recordingWriterObserver) ObserveIngestDLQ(result string) {
	o.dlqResult = result
}

type recordingStore struct {
	calls     int
	committed bool
	err       error
	rows      []int
	onApply   func()
}

func (s *recordingStore) ApplyIngestBatch(_ context.Context, commands []Command) error {
	s.calls++
	s.rows = append(s.rows, len(commands))
	if s.err == nil {
		s.committed = true
	}
	if s.onApply != nil {
		s.onApply()
	}
	return s.err
}

type recordingMessage struct {
	data            []byte
	store           *recordingStore
	acked           bool
	nacked          bool
	termed          bool
	ackBeforeCommit bool
	numDelivered    uint64
}

func (m *recordingMessage) Data() []byte { return m.data }
func (m *recordingMessage) Metadata() (*jetstream.MsgMetadata, error) {
	return &jetstream.MsgMetadata{NumDelivered: max(m.numDelivered, 1)}, nil
}
func (m *recordingMessage) Headers() nats.Header             { return nil }
func (m *recordingMessage) Subject() string                  { return "" }
func (m *recordingMessage) Reply() string                    { return "" }
func (m *recordingMessage) Ack() error                       { return nil }
func (m *recordingMessage) NakWithDelay(time.Duration) error { return m.Nak() }
func (m *recordingMessage) InProgress() error                { return nil }
func (m *recordingMessage) TermWithReason(string) error      { return m.Term() }
func (m *recordingMessage) DoubleAck(context.Context) error {
	m.acked = true
	m.ackBeforeCommit = !m.store.committed
	return nil
}
func (m *recordingMessage) Nak() error  { m.nacked = true; return nil }
func (m *recordingMessage) Term() error { m.termed = true; return nil }

func encodedMessage(t *testing.T) *recordingMessage {
	t.Helper()
	body, err := json.Marshal(validCommand())
	if err != nil {
		t.Fatal(err)
	}
	return &recordingMessage{data: body}
}

func encodedMessages(t *testing.T, store *recordingStore, count int) []*recordingMessage {
	t.Helper()
	messages := make([]*recordingMessage, count)
	for i := range messages {
		messages[i] = encodedMessage(t)
		messages[i].store = store
	}
	return messages
}

type fetchStep struct {
	messages       []*recordingMessage
	waitForContext bool
	err            error
	onFetch        func()
}

type scriptedFetcher struct {
	steps []fetchStep
	calls int
}

func (f *scriptedFetcher) fetch(ctx context.Context, size int) (jetstream.MessageBatch, error) {
	if f.calls >= len(f.steps) {
		return nil, fmt.Errorf("unexpected fetch %d", f.calls+1)
	}
	step := f.steps[f.calls]
	f.calls++
	if len(step.messages) > size {
		return nil, fmt.Errorf("step has %d messages for fetch size %d", len(step.messages), size)
	}
	batch := &scriptedMessageBatch{messages: make(chan jetstream.Msg, len(step.messages))}
	for _, message := range step.messages {
		batch.messages <- message
	}
	if step.onFetch != nil {
		step.onFetch()
	}
	if step.waitForContext {
		go func() {
			<-ctx.Done()
			batch.mu.Lock()
			batch.err = ctx.Err()
			batch.mu.Unlock()
			close(batch.messages)
		}()
	} else {
		batch.mu.Lock()
		batch.err = step.err
		batch.mu.Unlock()
		close(batch.messages)
	}
	return batch, nil
}

type scriptedMessageBatch struct {
	messages chan jetstream.Msg
	mu       sync.RWMutex
	err      error
}

func (b *scriptedMessageBatch) Messages() <-chan jetstream.Msg { return b.messages }
func (b *scriptedMessageBatch) Error() error {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.err
}

func testRunWriter(
	store *recordingStore,
	fetcher *scriptedFetcher,
	batchSize int,
	flushInterval time.Duration,
) *Writer {
	return &Writer{
		store: store,
		config: WriterConfig{
			BatchSize:     batchSize,
			FlushInterval: flushInterval,
			AckWait:       time.Second,
		},
		fetch: fetcher.fetch,
	}
}

type recordingDLQPublisher struct {
	subject string
	payload []byte
}

func (p *recordingDLQPublisher) Publish(
	_ context.Context,
	subject string,
	payload []byte,
	_ ...jetstream.PublishOpt,
) (*jetstream.PubAck, error) {
	p.subject = subject
	p.payload = append([]byte(nil), payload...)
	return &jetstream.PubAck{Stream: "DEFERMQ_INGEST"}, nil
}
