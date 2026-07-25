package ingest

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/defermq/defermq/internal/domain"
	"github.com/google/uuid"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func TestIDsDeterministicForIdempotencyKey(t *testing.T) {
	key := "order-42"
	delivery1, payload1, err := IDs(&key)
	if err != nil {
		t.Fatal(err)
	}
	delivery2, payload2, err := IDs(&key)
	if err != nil {
		t.Fatal(err)
	}
	if delivery1 != delivery2 || payload1 != payload2 || delivery1 == payload1 {
		t.Fatalf("IDs are not stable and distinct: %s %s / %s %s", delivery1, payload1, delivery2, payload2)
	}
	if delivery1.Version() != 5 || payload1.Version() != 5 {
		t.Fatalf("deterministic IDs must be UUIDv5: %v %v", delivery1.Version(), payload1.Version())
	}
}

func TestIDsUseUUIDv7WithoutKey(t *testing.T) {
	delivery, payload, err := IDs(nil)
	if err != nil {
		t.Fatal(err)
	}
	if delivery.Version() != 7 || payload.Version() != 7 || delivery == payload {
		t.Fatalf("unexpected generated IDs: %s %s", delivery, payload)
	}
}

func TestBatchPublisherWaitsForPubAck(t *testing.T) {
	js := &fakeAsyncPublisher{}
	publisher, err := NewBatchPublisher(js, "defermq.ingest.commands", 2, time.Second, 2)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runDone := make(chan error, 1)
	go func() { runDone <- publisher.Run(ctx) }()

	results := make(chan error, 2)
	for i := 0; i < 2; i++ {
		command := validCommand()
		command.CommandID = uuid.New()
		go func() { results <- publisher.Publish(ctx, command) }()
	}
	for i := 0; i < 2; i++ {
		select {
		case err := <-results:
			if err != nil {
				t.Fatal(err)
			}
		case <-time.After(time.Second):
			t.Fatal("Publish did not complete after PubAck")
		}
	}
	js.mu.Lock()
	defer js.mu.Unlock()
	if len(js.messages) != 2 {
		t.Fatalf("published %d messages, want 2", len(js.messages))
	}
	var decoded Command
	if err := json.Unmarshal(js.messages[0].Data, &decoded); err != nil || decoded.Kind != KindCreate {
		t.Fatalf("invalid published command: %+v, %v", decoded, err)
	}
}

func TestDeliveryCommandsUseOneStableShard(t *testing.T) {
	id := uuid.MustParse("018f0f76-7ea2-7b30-8f65-8f43f5dd1396")
	subject := ShardSubject("defermq.ingest.commands", id, DefaultShardCount)
	if subject != ShardSubject("defermq.ingest.commands", id, DefaultShardCount) {
		t.Fatal("shard subject is not stable")
	}
	if subject == ShardSubject("defermq.ingest.commands", uuid.New(), DefaultShardCount) {
		t.Log("hash collisions are allowed; delivery identity still determines the shard")
	}
	if got := StreamSubject("defermq.ingest.commands"); got != "defermq.ingest.commands.*" {
		t.Fatalf("unexpected stream subject %q", got)
	}
}

func TestBatchPublisherPreservesEnqueueOrderWithinShard(t *testing.T) {
	js := &fakeAsyncPublisher{}
	publisher, err := NewBatchPublisher(js, "defermq.ingest.commands", 3, time.Second, 3)
	if err != nil {
		t.Fatal(err)
	}
	id := uuid.New()
	kinds := []Kind{KindCreate, KindReschedule, KindCancel}
	batch := make([]publishItem, 0, len(kinds))
	for _, kind := range kinds {
		batch = append(batch, publishItem{
			ctx: context.Background(), command: Command{
				Kind: kind, CommandID: uuid.New(), DeliveryID: id,
			}, result: make(chan publishResult, 1),
		})
	}
	publisher.flush(batch)
	for _, item := range batch {
		if result := <-item.result; result.err != nil {
			t.Fatal(result.err)
		}
	}
	js.mu.Lock()
	defer js.mu.Unlock()
	for i, message := range js.messages {
		if message.Subject != ShardSubject("defermq.ingest.commands", id, DefaultShardCount) {
			t.Fatalf("message %d used subject %q", i, message.Subject)
		}
		var command Command
		if err := json.Unmarshal(message.Data, &command); err != nil || command.Kind != kinds[i] {
			t.Fatalf("message %d order mismatch: %+v, %v", i, command, err)
		}
	}
}

func validCommand() Command {
	return Command{
		SchemaVersion: SchemaVersion, Kind: KindCreate, CommandID: uuid.New(),
		DeliveryID: uuid.New(), PayloadID: uuid.New(), DeliverAt: time.Now().Add(time.Minute),
		DestinationType: domain.DestinationHTTP, Destination: json.RawMessage(`{"type":"http"}`),
		MaxAttempts: 3, HotHorizon: time.Minute,
		Payload: &Payload{Body: []byte("x"), ContentType: "text/plain", SizeBytes: 1},
	}
}

type fakeAsyncPublisher struct {
	mu       sync.Mutex
	messages []*nats.Msg
}

func (p *fakeAsyncPublisher) PublishMsgAsync(msg *nats.Msg, _ ...jetstream.PublishOpt) (jetstream.PubAckFuture, error) {
	p.mu.Lock()
	p.messages = append(p.messages, msg)
	p.mu.Unlock()
	ok := make(chan *jetstream.PubAck, 1)
	ok <- &jetstream.PubAck{Stream: "DEFERMQ_INGEST", Sequence: 1}
	return &fakeFuture{message: msg, ok: ok, err: make(chan error)}, nil
}

type fakeFuture struct {
	message *nats.Msg
	ok      <-chan *jetstream.PubAck
	err     <-chan error
}

func (f *fakeFuture) Ok() <-chan *jetstream.PubAck { return f.ok }
func (f *fakeFuture) Err() <-chan error            { return f.err }
func (f *fakeFuture) Msg() *nats.Msg               { return f.message }
