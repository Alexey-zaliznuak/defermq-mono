package valkey

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

const DefaultBuckets = 32

var (
	ErrInvalidBucket = errors.New("invalid hot-index bucket")
	ErrLeaseLost     = errors.New("hot-index bucket lease lost")
	ErrCorruptState  = errors.New("corrupt hot-index state")
)

// Client is the subset of go-redis used by Store. Both redis.Client and
// redis.ClusterClient implement it.
type Client interface {
	SetNX(context.Context, string, interface{}, time.Duration) *redis.BoolCmd
	Eval(context.Context, string, []string, ...interface{}) *redis.Cmd
	HGetAll(context.Context, string) *redis.MapStringStringCmd
}

type cardinalityClient interface {
	ZCard(context.Context, string) *redis.IntCmd
}

type Config struct {
	Prefix  string
	Buckets int
}

func (c Config) withDefaults() Config {
	if c.Prefix == "" {
		c.Prefix = "defermq:hot"
	}
	if c.Buckets == 0 {
		c.Buckets = DefaultBuckets
	}
	return c
}

func (c Config) validate() error {
	if c.Buckets <= 0 {
		return errors.New("hot-index bucket count must be positive")
	}
	if strings.ContainsAny(c.Prefix, "{} \t\r\n") {
		return errors.New("hot-index prefix must not contain braces or whitespace")
	}
	return nil
}

func (c Config) Validate() error {
	c = c.withDefaults()
	return c.validate()
}

type Store struct {
	client  Client
	prefix  string
	buckets int
}

func New(client Client, cfg Config) (*Store, error) {
	if client == nil {
		return nil, errors.New("Valkey client is required")
	}
	cfg = cfg.withDefaults()
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	return &Store{client: client, prefix: cfg.Prefix, buckets: cfg.Buckets}, nil
}

func (s *Store) BucketCount() int {
	return s.buckets
}

// BucketFor uses FNV-1a over the UUID bytes. Its result is stable across
// processes and Go versions.
func (s *Store) BucketFor(id uuid.UUID) int {
	hash := fnv.New64a()
	_, _ = hash.Write(id[:])
	return int(hash.Sum64() % uint64(s.buckets))
}

type BucketKeys struct {
	Schedule    string
	Inflight    string
	OriginalDue string
	Lease       string
	State       string
}

func (s *Store) Keys(bucket int) (BucketKeys, error) {
	if err := s.validateBucket(bucket); err != nil {
		return BucketKeys{}, err
	}
	// Every key for one bucket carries the same Redis Cluster hash tag.
	tag := fmt.Sprintf("{hot:%d}", bucket)
	base := s.prefix + ":" + tag
	return BucketKeys{
		Schedule:    base + ":schedule",
		Inflight:    base + ":inflight",
		OriginalDue: base + ":due",
		Lease:       base + ":lease",
		State:       base + ":state",
	}, nil
}

func (s *Store) validateBucket(bucket int) error {
	if bucket < 0 || bucket >= s.buckets {
		return fmt.Errorf("%w: %d (bucket count %d)", ErrInvalidBucket, bucket, s.buckets)
	}
	return nil
}

type Entry struct {
	DeliveryID uuid.UUID
	Revision   int64
	DueAt      time.Time
}

// RepairRegisterResult is aligned with the corresponding input entry.
// Err is set only when that entry's bucket could not be registered.
type RepairRegisterResult struct {
	Inserted bool
	Err      error
}

func (e Entry) validate() error {
	if e.DeliveryID == uuid.Nil {
		return errors.New("delivery ID is required")
	}
	if e.Revision <= 0 {
		return errors.New("schedule revision must be positive")
	}
	if e.DueAt.IsZero() {
		return errors.New("due time is required")
	}
	return nil
}

func Member(id uuid.UUID, revision int64) (string, error) {
	entry := Entry{DeliveryID: id, Revision: revision, DueAt: time.UnixMilli(1)}
	if err := entry.validate(); err != nil {
		return "", err
	}
	return id.String() + ":" + strconv.FormatInt(revision, 10), nil
}

func ParseMember(member string) (uuid.UUID, int64, error) {
	idText, revisionText, ok := strings.Cut(member, ":")
	if !ok {
		return uuid.Nil, 0, fmt.Errorf("%w: invalid member %q", ErrCorruptState, member)
	}
	id, err := uuid.Parse(idText)
	if err != nil || id == uuid.Nil {
		return uuid.Nil, 0, fmt.Errorf("%w: invalid member UUID %q", ErrCorruptState, idText)
	}
	revision, err := strconv.ParseInt(revisionText, 10, 64)
	if err != nil || revision <= 0 {
		return uuid.Nil, 0, fmt.Errorf("%w: invalid member revision %q", ErrCorruptState, revisionText)
	}
	return id, revision, nil
}

type ClaimedEntry struct {
	Entry
	LeaseUntil time.Time
}

type BucketState struct {
	Bucket      int
	Owner       string
	Status      string
	HeartbeatAt time.Time
}
