package valkey

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"
)

const heartbeatScript = `
if redis.call("GET", KEYS[2]) ~= ARGV[1] then
  return redis.error_reply("LEASE_LOST")
end
local nowParts = redis.call("TIME")
local now = tonumber(nowParts[1]) * 1000 + math.floor(tonumber(nowParts[2]) / 1000)
redis.call("HSET", KEYS[1],
  "owner", ARGV[2],
  "status", ARGV[3],
  "heartbeat_unix_ms", now)
return now
`

// Heartbeat updates scheduler state using server time and is fenced by the
// current bucket lease.
func (s *Store) Heartbeat(ctx context.Context, bucket int, token, owner, status string) (time.Time, error) {
	if token == "" {
		return time.Time{}, errors.New("lease token is required")
	}
	if owner == "" {
		return time.Time{}, errors.New("heartbeat owner is required")
	}
	if status == "" {
		return time.Time{}, errors.New("heartbeat status is required")
	}
	keys, err := s.Keys(bucket)
	if err != nil {
		return time.Time{}, err
	}
	serverMS, err := s.client.Eval(
		ctx,
		heartbeatScript,
		[]string{keys.State, keys.Lease},
		token,
		owner,
		status,
	).Int64()
	if err != nil {
		return time.Time{}, mapScriptError("update bucket heartbeat", err)
	}
	return time.UnixMilli(serverMS).UTC(), nil
}

func (s *Store) BucketState(ctx context.Context, bucket int) (BucketState, bool, error) {
	keys, err := s.Keys(bucket)
	if err != nil {
		return BucketState{}, false, err
	}
	fields, err := s.client.HGetAll(ctx, keys.State).Result()
	if err != nil {
		return BucketState{}, false, fmt.Errorf("read bucket %d state: %w", bucket, err)
	}
	if len(fields) == 0 {
		return BucketState{}, false, nil
	}
	heartbeatMS, err := strconv.ParseInt(fields["heartbeat_unix_ms"], 10, 64)
	if err != nil {
		return BucketState{}, false, fmt.Errorf("%w: bucket %d heartbeat is invalid", ErrCorruptState, bucket)
	}
	if fields["owner"] == "" || fields["status"] == "" {
		return BucketState{}, false, fmt.Errorf("%w: bucket %d state is incomplete", ErrCorruptState, bucket)
	}
	return BucketState{
		Bucket:      bucket,
		Owner:       fields["owner"],
		Status:      fields["status"],
		HeartbeatAt: time.UnixMilli(heartbeatMS).UTC(),
	}, true, nil
}
