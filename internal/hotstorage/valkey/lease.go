package valkey

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"time"
)

const renewLeaseScript = `
if redis.call("GET", KEYS[1]) ~= ARGV[1] then
  return 0
end
return redis.call("PEXPIRE", KEYS[1], ARGV[2])
`

const releaseLeaseScript = `
if redis.call("GET", KEYS[1]) ~= ARGV[1] then
  return 0
end
return redis.call("DEL", KEYS[1])
`

// AcquireLease obtains the per-bucket scheduler lease with SET NX PX. The
// returned cryptographically random token must be used for renew/release and
// all lease-fenced bucket mutations.
func (s *Store) AcquireLease(ctx context.Context, bucket int, ttl time.Duration) (token string, acquired bool, err error) {
	if err := validTTL(ttl); err != nil {
		return "", false, err
	}
	keys, err := s.Keys(bucket)
	if err != nil {
		return "", false, err
	}
	token, err = newToken()
	if err != nil {
		return "", false, err
	}
	acquired, err = s.client.SetNX(ctx, keys.Lease, token, ttl).Result()
	if err != nil {
		return "", false, fmt.Errorf("acquire bucket %d lease: %w", bucket, err)
	}
	if !acquired {
		return "", false, nil
	}
	return token, true, nil
}

func (s *Store) RenewLease(ctx context.Context, bucket int, token string, ttl time.Duration) error {
	if token == "" {
		return errors.New("lease token is required")
	}
	if err := validTTL(ttl); err != nil {
		return err
	}
	keys, err := s.Keys(bucket)
	if err != nil {
		return err
	}
	result, err := s.client.Eval(ctx, renewLeaseScript, []string{keys.Lease}, token, ttl.Milliseconds()).Int64()
	if err != nil {
		return fmt.Errorf("renew bucket %d lease: %w", bucket, err)
	}
	if result != 1 {
		return ErrLeaseLost
	}
	return nil
}

func (s *Store) ReleaseLease(ctx context.Context, bucket int, token string) error {
	if token == "" {
		return errors.New("lease token is required")
	}
	keys, err := s.Keys(bucket)
	if err != nil {
		return err
	}
	result, err := s.client.Eval(ctx, releaseLeaseScript, []string{keys.Lease}, token).Int64()
	if err != nil {
		return fmt.Errorf("release bucket %d lease: %w", bucket, err)
	}
	if result != 1 {
		return ErrLeaseLost
	}
	return nil
}

func validTTL(ttl time.Duration) error {
	if ttl < time.Millisecond {
		return errors.New("lease TTL must be at least one millisecond")
	}
	return nil
}

func newToken() (string, error) {
	var raw [32]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", fmt.Errorf("generate lease token: %w", err)
	}
	return hex.EncodeToString(raw[:]), nil
}
