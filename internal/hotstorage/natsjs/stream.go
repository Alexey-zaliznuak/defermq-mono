package natsjs

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

type StreamConfig struct {
	Name            string
	Subjects        Subjects
	Replicas        int
	MaxAge          time.Duration
	MaxBytes        int64
	MaxMsgSize      int32
	DuplicateWindow time.Duration
}

func (c StreamConfig) Desired() jetstream.StreamConfig {
	return jetstream.StreamConfig{
		Name:              c.Name,
		Subjects:          []string{c.Subjects.ScheduleWildcard(), c.Subjects.ReadyWildcard()},
		Retention:         jetstream.LimitsPolicy,
		Storage:           jetstream.FileStorage,
		Replicas:          c.Replicas,
		MaxAge:            c.MaxAge,
		MaxBytes:          c.MaxBytes,
		MaxMsgSize:        c.MaxMsgSize,
		Duplicates:        c.DuplicateWindow,
		AllowMsgSchedules: true,
	}
}

func EnsureStream(ctx context.Context, js jetstream.JetStream, cfg StreamConfig) (jetstream.Stream, error) {
	desired := cfg.Desired()
	stream, err := js.Stream(ctx, cfg.Name)
	if errors.Is(err, jetstream.ErrStreamNotFound) {
		created, createErr := js.CreateStream(ctx, desired)
		if createErr != nil {
			return nil, fmt.Errorf("create JetStream stream %q: %w", cfg.Name, createErr)
		}
		return created, nil
	}
	if err != nil {
		return nil, fmt.Errorf("load JetStream stream %q: %w", cfg.Name, err)
	}
	info, err := stream.Info(ctx)
	if err != nil {
		return nil, fmt.Errorf("read JetStream stream %q: %w", cfg.Name, err)
	}
	merged, changed, err := CompatibleStreamConfig(info.Config, desired)
	if err != nil {
		return nil, fmt.Errorf("stream %q is incompatible: %w", cfg.Name, err)
	}
	if !changed {
		return stream, nil
	}
	updated, err := js.UpdateStream(ctx, merged)
	if err != nil {
		return nil, fmt.Errorf("update JetStream stream %q: %w", cfg.Name, err)
	}
	return updated, nil
}

func CheckStream(ctx context.Context, js jetstream.JetStream, cfg StreamConfig) error {
	stream, err := js.Stream(ctx, cfg.Name)
	if err != nil {
		return fmt.Errorf("load JetStream stream %q: %w", cfg.Name, err)
	}
	info, err := stream.Info(ctx)
	if err != nil {
		return fmt.Errorf("read JetStream stream %q: %w", cfg.Name, err)
	}
	_, changed, err := CompatibleStreamConfig(info.Config, cfg.Desired())
	if err != nil {
		return err
	}
	if changed {
		return errors.New("stream configuration drift requires reconciliation")
	}
	return nil
}

// CompatibleStreamConfig returns an additive, non-shrinking update.
func CompatibleStreamConfig(current, desired jetstream.StreamConfig) (jetstream.StreamConfig, bool, error) {
	if current.Name != desired.Name {
		return current, false, errors.New("stream name differs")
	}
	if current.Sealed {
		return current, false, errors.New("stream is sealed")
	}
	if current.Storage != jetstream.FileStorage {
		return current, false, fmt.Errorf("storage is %v, file storage is required", current.Storage)
	}
	if current.Retention != jetstream.LimitsPolicy {
		return current, false, fmt.Errorf("retention is %v, limits retention is required", current.Retention)
	}
	if current.NoAck {
		return current, false, errors.New("PubAck is disabled")
	}
	if current.Mirror != nil || len(current.Sources) != 0 {
		return current, false, errors.New("mirrors and sources cannot host message schedules")
	}

	merged := current
	changed := false
	for _, subject := range desired.Subjects {
		if !slices.Contains(merged.Subjects, subject) {
			merged.Subjects = append(merged.Subjects, subject)
			changed = true
		}
	}
	if !merged.AllowMsgSchedules {
		merged.AllowMsgSchedules = true
		changed = true
	}
	if merged.Replicas < desired.Replicas {
		merged.Replicas = desired.Replicas
		changed = true
	}
	if shouldIncreaseDuration(merged.MaxAge, desired.MaxAge) {
		merged.MaxAge = desired.MaxAge
		changed = true
	}
	if shouldIncreaseInt64(merged.MaxBytes, desired.MaxBytes) {
		merged.MaxBytes = desired.MaxBytes
		changed = true
	}
	if shouldIncreaseInt32(merged.MaxMsgSize, desired.MaxMsgSize) {
		merged.MaxMsgSize = desired.MaxMsgSize
		changed = true
	}
	if merged.Duplicates < desired.Duplicates {
		merged.Duplicates = desired.Duplicates
		changed = true
	}
	return merged, changed, nil
}

func shouldIncreaseDuration(current, desired time.Duration) bool {
	return current > 0 && desired > current
}

func shouldIncreaseInt64(current, desired int64) bool {
	return current >= 0 && desired > current
}

func shouldIncreaseInt32(current, desired int32) bool {
	return current >= 0 && desired > current
}
