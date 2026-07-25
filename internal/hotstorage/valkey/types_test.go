package valkey

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

func TestConfigDefaultsAndValidation(t *testing.T) {
	client := &stubClient{}
	store, err := New(client, Config{})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	if store.BucketCount() != 32 {
		t.Fatalf("BucketCount() = %d, want 32", store.BucketCount())
	}

	for _, cfg := range []Config{
		{Buckets: -1},
		{Prefix: "bad{prefix"},
		{Prefix: "bad prefix"},
	} {
		if _, err := New(client, cfg); err == nil {
			t.Fatalf("New(%+v) accepted invalid config", cfg)
		}
	}
}

func TestBucketForIsStableAndConfigurable(t *testing.T) {
	id := uuid.MustParse("018f6544-7c00-7000-8000-000000000001")
	store32, _ := New(&stubClient{}, Config{Buckets: 32})
	store7, _ := New(&stubClient{}, Config{Buckets: 7})

	if got, want := store32.BucketFor(id), 15; got != want {
		t.Fatalf("32-bucket hash = %d, want stable value %d", got, want)
	}
	if got, want := store7.BucketFor(id), 0; got != want {
		t.Fatalf("7-bucket hash = %d, want stable value %d", got, want)
	}
	for i := 0; i < 100; i++ {
		if got := store7.BucketFor(uuid.New()); got < 0 || got >= 7 {
			t.Fatalf("BucketFor() = %d, outside configured range", got)
		}
	}
}

func TestKeysShareClusterHashTag(t *testing.T) {
	store, _ := New(&stubClient{}, Config{Prefix: "test", Buckets: 32})
	keys, err := store.Keys(9)
	if err != nil {
		t.Fatal(err)
	}
	for name, key := range map[string]string{
		"schedule": keys.Schedule, "inflight": keys.Inflight, "due": keys.OriginalDue,
		"lease": keys.Lease, "state": keys.State,
	} {
		if !strings.Contains(key, "{hot:9}") {
			t.Errorf("%s key %q lacks bucket hash tag", name, key)
		}
	}
	if _, err := store.Keys(-1); err == nil {
		t.Fatal("Keys(-1) succeeded")
	}
	if _, err := store.Keys(32); err == nil {
		t.Fatal("Keys(bucket count) succeeded")
	}
}

func TestMemberRoundTripAndValidation(t *testing.T) {
	id := uuid.MustParse("018f6544-7c00-7000-8000-000000000001")
	member, err := Member(id, 42)
	if err != nil {
		t.Fatal(err)
	}
	gotID, gotRevision, err := ParseMember(member)
	if err != nil {
		t.Fatal(err)
	}
	if gotID != id || gotRevision != 42 {
		t.Fatalf("ParseMember() = %s, %d", gotID, gotRevision)
	}
	for _, invalid := range []string{"", "not-a-uuid:1", id.String() + ":0", id.String() + ":x", id.String() + ":1:2"} {
		if _, _, err := ParseMember(invalid); err == nil {
			t.Errorf("ParseMember(%q) succeeded", invalid)
		}
	}
	if _, err := Member(uuid.Nil, 1); err == nil {
		t.Fatal("Member(nil UUID) succeeded")
	}
	if _, err := Member(id, 0); err == nil {
		t.Fatal("Member(zero revision) succeeded")
	}
}

// stubClient is only used by tests that never issue Redis commands.
type stubClient struct{}

func (*stubClient) SetNX(_ context.Context, _ string, _ interface{}, _ time.Duration) *redis.BoolCmd {
	panic("unexpected command")
}
func (*stubClient) Eval(_ context.Context, _ string, _ []string, _ ...interface{}) *redis.Cmd {
	panic("unexpected command")
}
func (*stubClient) HGetAll(_ context.Context, _ string) *redis.MapStringStringCmd {
	panic("unexpected command")
}
