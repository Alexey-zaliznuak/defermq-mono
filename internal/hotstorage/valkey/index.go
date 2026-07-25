package valkey

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
)

const registerScript = `
if redis.call("ZSCORE", KEYS[1], ARGV[1]) or redis.call("ZSCORE", KEYS[2], ARGV[1]) then
  return 0
end
redis.call("ZADD", KEYS[1], ARGV[2], ARGV[1])
return 1
`

const registerBatchScript = `
local result = {}
for i = 1, #ARGV, 2 do
  local member = ARGV[i]
  if redis.call("ZSCORE", KEYS[1], member) or redis.call("ZSCORE", KEYS[2], member) then
    table.insert(result, 0)
  else
    redis.call("ZADD", KEYS[1], ARGV[i + 1], member)
    table.insert(result, 1)
  end
end
return result
`

const repairRegisterBatchConcurrency = 8

const claimScript = `
if redis.call("GET", KEYS[4]) ~= ARGV[1] then
  return redis.error_reply("LEASE_LOST")
end
local nowParts = redis.call("TIME")
local now = tonumber(nowParts[1]) * 1000 + math.floor(tonumber(nowParts[2]) / 1000)
local cutoff = now + tonumber(ARGV[2])
local leaseUntil = now + tonumber(ARGV[3])
local members = redis.call("ZRANGEBYSCORE", KEYS[1], "-inf", cutoff, "LIMIT", 0, tonumber(ARGV[4]))
local result = {now}
for _, member in ipairs(members) do
  local due = redis.call("ZSCORE", KEYS[1], member)
  if due and redis.call("ZREM", KEYS[1], member) == 1 then
    redis.call("ZADD", KEYS[2], leaseUntil, member)
    redis.call("HSET", KEYS[3], member, due)
    table.insert(result, member)
    table.insert(result, due)
  end
end
return result
`

const completeScript = `
if redis.call("GET", KEYS[3]) ~= ARGV[1] then
  return redis.error_reply("LEASE_LOST")
end
local removed = redis.call("ZREM", KEYS[1], ARGV[2])
redis.call("HDEL", KEYS[2], ARGV[2])
return removed
`

const reclaimScript = `
if redis.call("GET", KEYS[4]) ~= ARGV[1] then
  return redis.error_reply("LEASE_LOST")
end
local nowParts = redis.call("TIME")
local now = tonumber(nowParts[1]) * 1000 + math.floor(tonumber(nowParts[2]) / 1000)
local members = redis.call("ZRANGEBYSCORE", KEYS[2], "-inf", now, "LIMIT", 0, tonumber(ARGV[2]))
for _, member in ipairs(members) do
  if not redis.call("HGET", KEYS[3], member) then
    return redis.error_reply("CORRUPT_INFLIGHT")
  end
end
local result = {now}
for _, member in ipairs(members) do
  local due = redis.call("HGET", KEYS[3], member)
  if redis.call("ZREM", KEYS[2], member) == 1 then
    redis.call("ZADD", KEYS[1], due, member)
    redis.call("HDEL", KEYS[3], member)
    table.insert(result, member)
    table.insert(result, due)
  end
end
return result
`

// RepairRegister inserts a schedule member only when it is absent from both
// schedule and inflight. Repeating the call is safe.
func (s *Store) RepairRegister(ctx context.Context, entry Entry) (bool, error) {
	if err := entry.validate(); err != nil {
		return false, err
	}
	member, _ := Member(entry.DeliveryID, entry.Revision)
	bucket := s.BucketFor(entry.DeliveryID)
	keys, _ := s.Keys(bucket)
	result, err := s.client.Eval(
		ctx,
		registerScript,
		[]string{keys.Schedule, keys.Inflight},
		member,
		entry.DueAt.UnixMilli(),
	).Int64()
	if err != nil {
		return false, fmt.Errorf("repair-register delivery %s revision %d: %w", entry.DeliveryID, entry.Revision, err)
	}
	return result == 1, nil
}

// RepairRegisterBatch groups entries by bucket and registers each group with
// one atomic Lua call. Bucket calls run concurrently with a fixed upper bound.
// Results retain input order; a failed bucket sets Err only on its entries.
func (s *Store) RepairRegisterBatch(
	ctx context.Context,
	entries []Entry,
) ([]RepairRegisterResult, error) {
	results := make([]RepairRegisterResult, len(entries))
	if len(entries) == 0 {
		return results, nil
	}

	type bucketEntry struct {
		index  int
		member string
		score  int64
	}
	groups := make(map[int][]bucketEntry)
	for i, entry := range entries {
		if err := entry.validate(); err != nil {
			return nil, fmt.Errorf("repair-register batch entry %d: %w", i, err)
		}
		member, _ := Member(entry.DeliveryID, entry.Revision)
		bucket := s.BucketFor(entry.DeliveryID)
		groups[bucket] = append(groups[bucket], bucketEntry{
			index: i, member: member, score: entry.DueAt.UnixMilli(),
		})
	}

	var (
		wg       sync.WaitGroup
		failures = make(chan error, len(groups))
		limit    = make(chan struct{}, repairRegisterBatchConcurrency)
	)
	for bucket, group := range groups {
		bucket, group := bucket, group
		wg.Add(1)
		go func() {
			defer wg.Done()
			select {
			case limit <- struct{}{}:
				defer func() { <-limit }()
			case <-ctx.Done():
				err := ctx.Err()
				for _, item := range group {
					results[item.index].Err = err
				}
				failures <- err
				return
			}

			keys, _ := s.Keys(bucket)
			args := make([]interface{}, 0, len(group)*2)
			for _, item := range group {
				args = append(args, item.member, item.score)
			}
			raw, err := s.client.Eval(
				ctx, registerBatchScript, []string{keys.Schedule, keys.Inflight}, args...,
			).Result()
			if err != nil {
				err = fmt.Errorf("repair-register bucket %d: %w", bucket, err)
				for _, item := range group {
					results[item.index].Err = err
				}
				failures <- err
				return
			}
			values, err := resultSlice(raw)
			if err == nil && len(values) != len(group) {
				err = fmt.Errorf(
					"%w: repair-register bucket %d returned %d results for %d entries",
					ErrCorruptState, bucket, len(values), len(group),
				)
			}
			if err != nil {
				for _, item := range group {
					results[item.index].Err = err
				}
				failures <- err
				return
			}
			for i, value := range values {
				inserted, parseErr := resultInt64(value)
				if parseErr != nil || (inserted != 0 && inserted != 1) {
					err := fmt.Errorf(
						"%w: invalid repair-register result %v", ErrCorruptState, value,
					)
					for _, item := range group {
						results[item.index].Err = err
					}
					failures <- err
					return
				}
				results[group[i].index].Inserted = inserted == 1
			}
		}()
	}
	wg.Wait()
	close(failures)

	var errs []error
	for err := range failures {
		errs = append(errs, err)
	}
	return results, errors.Join(errs...)
}

// ClaimDue atomically moves due schedule members into inflight. Due selection
// uses Valkey's TIME plus earlyWindow, never the scheduler host clock.
func (s *Store) ClaimDue(
	ctx context.Context,
	bucket int,
	token string,
	earlyWindow time.Duration,
	inflightTTL time.Duration,
	limit int,
) ([]ClaimedEntry, error) {
	if token == "" {
		return nil, errors.New("lease token is required")
	}
	if earlyWindow < 0 {
		return nil, errors.New("early window must not be negative")
	}
	if err := validTTL(inflightTTL); err != nil {
		return nil, fmt.Errorf("invalid inflight TTL: %w", err)
	}
	if limit <= 0 {
		return nil, errors.New("claim limit must be positive")
	}
	keys, err := s.Keys(bucket)
	if err != nil {
		return nil, err
	}
	raw, err := s.client.Eval(
		ctx,
		claimScript,
		[]string{keys.Schedule, keys.Inflight, keys.OriginalDue, keys.Lease},
		token,
		earlyWindow.Milliseconds(),
		inflightTTL.Milliseconds(),
		limit,
	).Result()
	if err != nil {
		return nil, mapScriptError("claim due", err)
	}
	values, err := resultSlice(raw)
	if err != nil {
		return nil, err
	}
	if len(values) == 0 {
		return nil, fmt.Errorf("%w: claim response has no server time", ErrCorruptState)
	}
	serverMS, err := resultInt64(values[0])
	if err != nil {
		return nil, fmt.Errorf("%w: invalid claim server time: %v", ErrCorruptState, err)
	}
	entries, err := parseEntries(values[1:])
	if err != nil {
		return nil, err
	}
	leaseUntil := time.UnixMilli(serverMS + inflightTTL.Milliseconds()).UTC()
	claimed := make([]ClaimedEntry, len(entries))
	for i, entry := range entries {
		claimed[i] = ClaimedEntry{Entry: entry, LeaseUntil: leaseUntil}
	}
	return claimed, nil
}

// Complete removes a claimed revision. It is idempotent; false means the
// member was no longer inflight.
func (s *Store) Complete(ctx context.Context, id uuid.UUID, revision int64, token string) (bool, error) {
	if token == "" {
		return false, errors.New("lease token is required")
	}
	member, err := Member(id, revision)
	if err != nil {
		return false, err
	}
	bucket := s.BucketFor(id)
	keys, _ := s.Keys(bucket)
	result, err := s.client.Eval(
		ctx,
		completeScript,
		[]string{keys.Inflight, keys.OriginalDue, keys.Lease},
		token,
		member,
	).Int64()
	if err != nil {
		return false, mapScriptError("complete delivery", err)
	}
	return result == 1, nil
}

// ReclaimExpired moves expired inflight members back to schedule with their
// exact original due scores.
func (s *Store) ReclaimExpired(ctx context.Context, bucket int, token string, limit int) ([]Entry, error) {
	if token == "" {
		return nil, errors.New("lease token is required")
	}
	if limit <= 0 {
		return nil, errors.New("reclaim limit must be positive")
	}
	keys, err := s.Keys(bucket)
	if err != nil {
		return nil, err
	}
	raw, err := s.client.Eval(
		ctx,
		reclaimScript,
		[]string{keys.Schedule, keys.Inflight, keys.OriginalDue, keys.Lease},
		token,
		limit,
	).Result()
	if err != nil {
		return nil, mapScriptError("reclaim expired", err)
	}
	values, err := resultSlice(raw)
	if err != nil {
		return nil, err
	}
	if len(values) == 0 {
		return nil, fmt.Errorf("%w: reclaim response has no server time", ErrCorruptState)
	}
	if _, err := resultInt64(values[0]); err != nil {
		return nil, fmt.Errorf("%w: invalid reclaim server time: %v", ErrCorruptState, err)
	}
	return parseEntries(values[1:])
}

// BucketDepth samples cardinalities with O(1) ZCARD commands. Callers can
// rotate through buckets instead of scanning all sorted-set members.
func (s *Store) BucketDepth(ctx context.Context, bucket int) (schedule, inflight int64, err error) {
	keys, err := s.Keys(bucket)
	if err != nil {
		return 0, 0, err
	}
	client, ok := s.client.(cardinalityClient)
	if !ok {
		return 0, 0, errors.New("Valkey client does not support cardinality sampling")
	}
	schedule, err = client.ZCard(ctx, keys.Schedule).Result()
	if err != nil {
		return 0, 0, fmt.Errorf("sample bucket %d depth: %w", bucket, err)
	}
	inflight, err = client.ZCard(ctx, keys.Inflight).Result()
	if err != nil {
		return 0, 0, fmt.Errorf("sample bucket %d depth: %w", bucket, err)
	}
	return schedule, inflight, nil
}

func parseEntries(values []interface{}) ([]Entry, error) {
	if len(values)%2 != 0 {
		return nil, fmt.Errorf("%w: odd member/score response length", ErrCorruptState)
	}
	entries := make([]Entry, 0, len(values)/2)
	for i := 0; i < len(values); i += 2 {
		member, ok := values[i].(string)
		if !ok {
			return nil, fmt.Errorf("%w: member has type %T", ErrCorruptState, values[i])
		}
		id, revision, err := ParseMember(member)
		if err != nil {
			return nil, err
		}
		dueMS, err := resultInt64(values[i+1])
		if err != nil {
			return nil, fmt.Errorf("%w: invalid due score: %v", ErrCorruptState, err)
		}
		entries = append(entries, Entry{DeliveryID: id, Revision: revision, DueAt: time.UnixMilli(dueMS).UTC()})
	}
	return entries, nil
}

func resultSlice(value interface{}) ([]interface{}, error) {
	values, ok := value.([]interface{})
	if !ok {
		return nil, fmt.Errorf("%w: script response has type %T", ErrCorruptState, value)
	}
	return values, nil
}

func resultInt64(value interface{}) (int64, error) {
	switch typed := value.(type) {
	case int64:
		return typed, nil
	case string:
		// ZSCORE and HGET are bulk strings, including integral scores as "123".
		if integer, err := strconv.ParseInt(typed, 10, 64); err == nil {
			return integer, nil
		}
		float, err := strconv.ParseFloat(typed, 64)
		if err != nil {
			return 0, err
		}
		return int64(float), nil
	default:
		return 0, fmt.Errorf("unexpected numeric type %T", value)
	}
}

func mapScriptError(operation string, err error) error {
	switch {
	case strings.Contains(err.Error(), "LEASE_LOST"):
		return fmt.Errorf("%s: %w", operation, ErrLeaseLost)
	case strings.Contains(err.Error(), "CORRUPT_INFLIGHT"):
		return fmt.Errorf("%s: %w", operation, ErrCorruptState)
	default:
		return fmt.Errorf("%s: %w", operation, err)
	}
}
