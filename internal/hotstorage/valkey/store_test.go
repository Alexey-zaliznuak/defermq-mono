package valkey

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

func TestLeaseIsExclusiveAndTokenFenced(t *testing.T) {
	store, server, client := newTestStore(t)
	ctx := context.Background()
	bucket := 3

	token, acquired, err := store.AcquireLease(ctx, bucket, 10*time.Second)
	if err != nil || !acquired || token == "" {
		t.Fatalf("AcquireLease() = %q, %v, %v", token, acquired, err)
	}
	if _, acquired, err := store.AcquireLease(ctx, bucket, time.Second); err != nil || acquired {
		t.Fatalf("second AcquireLease() acquired=%v err=%v", acquired, err)
	}
	if err := store.RenewLease(ctx, bucket, "wrong-token", 20*time.Second); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("wrong-token RenewLease() error = %v", err)
	}
	if err := store.ReleaseLease(ctx, bucket, "wrong-token"); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("wrong-token ReleaseLease() error = %v", err)
	}
	if err := store.RenewLease(ctx, bucket, token, 20*time.Second); err != nil {
		t.Fatalf("RenewLease() error = %v", err)
	}
	keys, _ := store.Keys(bucket)
	if ttl := server.TTL(keys.Lease); ttl != 20*time.Second {
		t.Fatalf("renewed TTL = %v, want 20s", ttl)
	}
	if err := store.ReleaseLease(ctx, bucket, token); err != nil {
		t.Fatalf("ReleaseLease() error = %v", err)
	}
	if exists := client.Exists(ctx, keys.Lease).Val(); exists != 0 {
		t.Fatal("lease key remains after release")
	}
}

func TestAcquireLeaseWithTenConcurrentSchedulers(t *testing.T) {
	store, _, _ := newTestStore(t)
	ctx := context.Background()
	var acquired atomic.Int32
	var wg sync.WaitGroup
	errs := make(chan error, 10)
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, ok, err := store.AcquireLease(ctx, 0, time.Minute)
			if err != nil {
				errs <- err
				return
			}
			if ok {
				acquired.Add(1)
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Errorf("AcquireLease() error = %v", err)
	}
	if got := acquired.Load(); got != 1 {
		t.Fatalf("successful concurrent acquisitions = %d, want 1", got)
	}
}

func TestRepairClaimAndComplete(t *testing.T) {
	store, server, client := newTestStore(t)
	ctx := context.Background()
	now := time.Date(2026, 7, 25, 18, 0, 0, 123_000_000, time.UTC)
	server.SetTime(now)
	due := now.Add(5 * time.Second)
	entry := Entry{DeliveryID: uuid.New(), Revision: 7, DueAt: due}
	bucket := store.BucketFor(entry.DeliveryID)

	if inserted, err := store.RepairRegister(ctx, entry); err != nil || !inserted {
		t.Fatalf("RepairRegister() = %v, %v", inserted, err)
	}
	if inserted, err := store.RepairRegister(ctx, entry); err != nil || inserted {
		t.Fatalf("repeated RepairRegister() = %v, %v", inserted, err)
	}
	token := acquireTestLease(t, store, bucket)
	claimed, err := store.ClaimDue(ctx, bucket, token, 5*time.Second, 30*time.Second, 100)
	if err != nil {
		t.Fatalf("ClaimDue() error = %v", err)
	}
	if len(claimed) != 1 || claimed[0].DeliveryID != entry.DeliveryID ||
		claimed[0].Revision != entry.Revision || !claimed[0].DueAt.Equal(due) {
		t.Fatalf("ClaimDue() = %+v", claimed)
	}
	if want := now.Add(30 * time.Second); !claimed[0].LeaseUntil.Equal(want) {
		t.Fatalf("LeaseUntil = %v, want %v", claimed[0].LeaseUntil, want)
	}
	if inserted, err := store.RepairRegister(ctx, entry); err != nil || inserted {
		t.Fatalf("inflight RepairRegister() = %v, %v", inserted, err)
	}

	keys, _ := store.Keys(bucket)
	member, _ := Member(entry.DeliveryID, entry.Revision)
	if score, err := server.ZScore(keys.Inflight, member); err != nil || int64(score) != now.Add(30*time.Second).UnixMilli() {
		t.Fatalf("inflight score = %v, %v", score, err)
	}
	if original, err := client.HGet(ctx, keys.OriginalDue, member).Int64(); err != nil || original != due.UnixMilli() {
		t.Fatalf("original due = %d, %v; want %d", original, err, due.UnixMilli())
	}
	if remaining := client.ZCard(ctx, keys.Schedule).Val(); remaining != 0 {
		t.Fatalf("schedule size after claim = %d", remaining)
	}

	if completed, err := store.Complete(ctx, entry.DeliveryID, entry.Revision, token); err != nil || !completed {
		t.Fatalf("Complete() = %v, %v", completed, err)
	}
	if completed, err := store.Complete(ctx, entry.DeliveryID, entry.Revision, token); err != nil || completed {
		t.Fatalf("repeated Complete() = %v, %v", completed, err)
	}
	if client.Exists(ctx, keys.OriginalDue).Val() != 0 || client.ZCard(ctx, keys.Inflight).Val() != 0 {
		t.Fatal("complete left inflight metadata")
	}
}

func TestRepairRegisterBatchMixedBucketsDuplicatesAndInflight(t *testing.T) {
	store, _, client := newTestStore(t)
	ctx := context.Background()
	now := time.Now().UTC()
	first := Entry{DeliveryID: uuid.New(), Revision: 1, DueAt: now.Add(time.Minute)}
	second := Entry{DeliveryID: uuid.New(), Revision: 2, DueAt: now.Add(2 * time.Minute)}
	for store.BucketFor(second.DeliveryID) == store.BucketFor(first.DeliveryID) {
		second.DeliveryID = uuid.New()
	}
	inflight := Entry{DeliveryID: uuid.New(), Revision: 3, DueAt: now.Add(3 * time.Minute)}
	for store.BucketFor(inflight.DeliveryID) == store.BucketFor(first.DeliveryID) {
		inflight.DeliveryID = uuid.New()
	}
	inflightKeys, _ := store.Keys(store.BucketFor(inflight.DeliveryID))
	inflightMember, _ := Member(inflight.DeliveryID, inflight.Revision)
	if err := client.ZAdd(ctx, inflightKeys.Inflight, redis.Z{
		Score: float64(now.Add(time.Minute).UnixMilli()), Member: inflightMember,
	}).Err(); err != nil {
		t.Fatal(err)
	}

	entries := []Entry{first, second, first, inflight}
	results, err := store.RepairRegisterBatch(ctx, entries)
	if err != nil {
		t.Fatalf("RepairRegisterBatch() error = %v", err)
	}
	if len(results) != len(entries) {
		t.Fatalf("result count = %d, want %d", len(results), len(entries))
	}
	wantInserted := []bool{true, true, false, false}
	for i, result := range results {
		if result.Err != nil || result.Inserted != wantInserted[i] {
			t.Errorf("result[%d] = %+v, want inserted=%v", i, result, wantInserted[i])
		}
	}
	for _, entry := range []Entry{first, second} {
		keys, _ := store.Keys(store.BucketFor(entry.DeliveryID))
		member, _ := Member(entry.DeliveryID, entry.Revision)
		score, err := client.ZScore(ctx, keys.Schedule, member).Result()
		if err != nil || int64(score) != entry.DueAt.UnixMilli() {
			t.Errorf("schedule member %s score = %v, %v", member, score, err)
		}
	}
	if score := client.ZScore(ctx, inflightKeys.Schedule, inflightMember); !errors.Is(score.Err(), redis.Nil) {
		t.Fatalf("inflight member was also scheduled: %v", score.Err())
	}
}

func TestRepairRegisterBatchReportsOnlyFailedBucket(t *testing.T) {
	server := miniredis.RunT(t)
	base := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = base.Close() })
	initial, err := New(base, Config{Prefix: "test", Buckets: 32})
	if err != nil {
		t.Fatal(err)
	}
	failed := Entry{DeliveryID: uuid.New(), Revision: 1, DueAt: time.Now().Add(time.Minute)}
	successful := Entry{DeliveryID: uuid.New(), Revision: 1, DueAt: time.Now().Add(2 * time.Minute)}
	for initial.BucketFor(successful.DeliveryID) == initial.BucketFor(failed.DeliveryID) {
		successful.DeliveryID = uuid.New()
	}
	failedKeys, _ := initial.Keys(initial.BucketFor(failed.DeliveryID))
	store, err := New(&failingEvalClient{
		Client: base, failSchedule: failedKeys.Schedule,
	}, Config{Prefix: "test", Buckets: 32})
	if err != nil {
		t.Fatal(err)
	}

	results, batchErr := store.RepairRegisterBatch(
		context.Background(), []Entry{failed, successful},
	)
	if batchErr == nil {
		t.Fatal("expected bucket error")
	}
	if len(results) != 2 || results[0].Err == nil || results[0].Inserted ||
		results[1].Err != nil || !results[1].Inserted {
		t.Fatalf("RepairRegisterBatch() results = %+v", results)
	}
	successKeys, _ := store.Keys(store.BucketFor(successful.DeliveryID))
	successMember, _ := Member(successful.DeliveryID, successful.Revision)
	if _, err := base.ZScore(context.Background(), successKeys.Schedule, successMember).Result(); err != nil {
		t.Fatalf("successful bucket was not registered: %v", err)
	}
	failedMember, _ := Member(failed.DeliveryID, failed.Revision)
	if err := base.ZScore(context.Background(), failedKeys.Schedule, failedMember).Err(); !errors.Is(err, redis.Nil) {
		t.Fatalf("failed bucket was modified: %v", err)
	}
}

type failingEvalClient struct {
	Client
	failSchedule string
}

func (c *failingEvalClient) Eval(
	ctx context.Context,
	script string,
	keys []string,
	args ...interface{},
) *redis.Cmd {
	if len(keys) > 0 && keys[0] == c.failSchedule {
		return redis.NewCmdResult(nil, errors.New("injected bucket failure"))
	}
	return c.Client.Eval(ctx, script, keys, args...)
}

func TestClaimUsesServerTimeAndEarlyWindow(t *testing.T) {
	store, server, _ := newTestStore(t)
	ctx := context.Background()
	now := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	server.SetTime(now)
	inside := Entry{DeliveryID: uuid.New(), Revision: 1, DueAt: now.Add(999 * time.Millisecond)}
	outside := Entry{DeliveryID: uuid.New(), Revision: 1, DueAt: now.Add(1001 * time.Millisecond)}
	for _, entry := range []Entry{inside, outside} {
		if _, err := store.RepairRegister(ctx, entry); err != nil {
			t.Fatal(err)
		}
	}
	// Put both deliveries in one bucket to exercise the cutoff exactly.
	bucket := store.BucketFor(inside.DeliveryID)
	for store.BucketFor(outside.DeliveryID) != bucket {
		outside.DeliveryID = uuid.New()
	}
	// Re-register after choosing an ID in the target bucket.
	if _, err := store.RepairRegister(ctx, outside); err != nil {
		t.Fatal(err)
	}
	token := acquireTestLease(t, store, bucket)
	claimed, err := store.ClaimDue(ctx, bucket, token, time.Second, time.Minute, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(claimed) != 1 || claimed[0].DeliveryID != inside.DeliveryID {
		t.Fatalf("claimed = %+v, want only inside-window entry", claimed)
	}
}

func TestReclaimRestoresOriginalDue(t *testing.T) {
	store, server, _ := newTestStore(t)
	ctx := context.Background()
	now := time.Date(2026, 3, 4, 5, 6, 7, 0, time.UTC)
	server.SetTime(now)
	entry := Entry{DeliveryID: uuid.New(), Revision: 2, DueAt: now.Add(-time.Hour)}
	if _, err := store.RepairRegister(ctx, entry); err != nil {
		t.Fatal(err)
	}
	bucket := store.BucketFor(entry.DeliveryID)
	token := acquireTestLease(t, store, bucket)
	if _, err := store.ClaimDue(ctx, bucket, token, 0, 10*time.Second, 1); err != nil {
		t.Fatal(err)
	}
	server.FastForward(11 * time.Second)
	server.SetTime(now.Add(11 * time.Second))

	reclaimed, err := store.ReclaimExpired(ctx, bucket, token, 10)
	if err != nil {
		t.Fatalf("ReclaimExpired() error = %v", err)
	}
	if len(reclaimed) != 1 || !reclaimed[0].DueAt.Equal(entry.DueAt) {
		t.Fatalf("ReclaimExpired() = %+v", reclaimed)
	}
	keys, _ := store.Keys(bucket)
	member, _ := Member(entry.DeliveryID, entry.Revision)
	score, err := server.ZScore(keys.Schedule, member)
	if err != nil || int64(score) != entry.DueAt.UnixMilli() {
		t.Fatalf("restored score = %v, %v; want %d", score, err, entry.DueAt.UnixMilli())
	}
	if server.Exists(keys.OriginalDue) {
		t.Fatal("original-due hash remains after reclaim")
	}
}

func TestClaimReclaimAndHeartbeatRequireCurrentLease(t *testing.T) {
	store, _, _ := newTestStore(t)
	ctx := context.Background()
	if _, err := store.ClaimDue(ctx, 1, "wrong", 0, time.Second, 1); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("ClaimDue() error = %v", err)
	}
	if _, err := store.ReclaimExpired(ctx, 1, "wrong", 1); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("ReclaimExpired() error = %v", err)
	}
	if _, err := store.Heartbeat(ctx, 1, "wrong", "scheduler-a", "active"); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("Heartbeat() error = %v", err)
	}
}

func TestBucketHeartbeatState(t *testing.T) {
	store, server, _ := newTestStore(t)
	ctx := context.Background()
	now := time.Date(2026, 6, 7, 8, 9, 10, 456_000_000, time.UTC)
	server.SetTime(now)
	token := acquireTestLease(t, store, 4)
	heartbeat, err := store.Heartbeat(ctx, 4, token, "scheduler-a", "active")
	if err != nil {
		t.Fatal(err)
	}
	if !heartbeat.Equal(now) {
		t.Fatalf("Heartbeat() = %v, want server time %v", heartbeat, now)
	}
	state, found, err := store.BucketState(ctx, 4)
	if err != nil || !found {
		t.Fatalf("BucketState() found=%v err=%v", found, err)
	}
	if state.Bucket != 4 || state.Owner != "scheduler-a" || state.Status != "active" || !state.HeartbeatAt.Equal(now) {
		t.Fatalf("BucketState() = %+v", state)
	}
	if _, found, err := store.BucketState(ctx, 5); err != nil || found {
		t.Fatalf("empty BucketState() found=%v err=%v", found, err)
	}
}

func TestBucketDepthUsesCardinality(t *testing.T) {
	store, _, _ := newTestStore(t)
	ctx := context.Background()
	entry := Entry{DeliveryID: uuid.New(), Revision: 1, DueAt: time.Now().Add(time.Minute)}
	if _, err := store.RepairRegister(ctx, entry); err != nil {
		t.Fatal(err)
	}
	bucket := store.BucketFor(entry.DeliveryID)
	schedule, inflight, err := store.BucketDepth(ctx, bucket)
	if err != nil {
		t.Fatal(err)
	}
	if schedule != 1 || inflight != 0 {
		t.Fatalf("BucketDepth() = (%d, %d), want (1, 0)", schedule, inflight)
	}
}

func newTestStore(t *testing.T) (*Store, *miniredis.Miniredis, *redis.Client) {
	t.Helper()
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	store, err := New(client, Config{Prefix: "test", Buckets: 32})
	if err != nil {
		t.Fatal(err)
	}
	return store, server, client
}

func acquireTestLease(t *testing.T, store *Store, bucket int) string {
	t.Helper()
	token, acquired, err := store.AcquireLease(context.Background(), bucket, time.Minute)
	if err != nil || !acquired {
		t.Fatalf("AcquireLease() acquired=%v err=%v", acquired, err)
	}
	return token
}
