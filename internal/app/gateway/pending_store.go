package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/defermq/defermq/internal/domain"
	"github.com/google/uuid"
	"github.com/nats-io/nats.go/jetstream"
)

type PendingStore interface {
	Get(context.Context, uuid.UUID) (PendingMessage, error)
	Put(context.Context, uuid.UUID, Message, uint64) error
	DeletePersisted(context.Context, uuid.UUID, int64) error
}

type PendingMessage struct {
	Message  Message `json:"message"`
	Sequence uint64  `json:"sequence"`
}

type NATSPendingStore struct {
	kv jetstream.KeyValue
}

func NewNATSPendingStore(kv jetstream.KeyValue) (*NATSPendingStore, error) {
	if kv == nil {
		return nil, errors.New("NATS pending KV is required")
	}
	return &NATSPendingStore{kv: kv}, nil
}

func (s *NATSPendingStore) Get(ctx context.Context, id uuid.UUID) (PendingMessage, error) {
	entry, err := s.kv.Get(ctx, id.String())
	if errors.Is(err, jetstream.ErrKeyNotFound) {
		return PendingMessage{}, domain.ErrNotFound
	}
	if err != nil {
		return PendingMessage{}, fmt.Errorf("get pending delivery: %w", err)
	}
	var message PendingMessage
	if err := json.Unmarshal(entry.Value(), &message); err != nil {
		return PendingMessage{}, fmt.Errorf("decode pending delivery: %w", err)
	}
	return message, nil
}

func (s *NATSPendingStore) Put(ctx context.Context, id uuid.UUID, message Message, sequence uint64) error {
	value, err := json.Marshal(PendingMessage{Message: message, Sequence: sequence})
	if err != nil {
		return fmt.Errorf("encode pending delivery: %w", err)
	}
	key := id.String()
	for {
		entry, getErr := s.kv.Get(ctx, key)
		if errors.Is(getErr, jetstream.ErrKeyNotFound) {
			if _, createErr := s.kv.Create(ctx, key, value); createErr == nil {
				return nil
			} else if errors.Is(createErr, jetstream.ErrKeyExists) {
				continue
			} else {
				return fmt.Errorf("create pending delivery: %w", createErr)
			}
		}
		if getErr != nil {
			return fmt.Errorf("get pending delivery for update: %w", getErr)
		}
		var current PendingMessage
		if err := json.Unmarshal(entry.Value(), &current); err != nil {
			return fmt.Errorf("decode pending delivery for update: %w", err)
		}
		if current.Sequence >= sequence {
			return nil
		}
		if _, updateErr := s.kv.Update(ctx, key, value, entry.Revision()); updateErr == nil {
			return nil
		} else if errors.Is(updateErr, jetstream.ErrKeyExists) {
			continue
		} else {
			return fmt.Errorf("update pending delivery: %w", updateErr)
		}
	}
}

func (s *NATSPendingStore) DeletePersisted(ctx context.Context, id uuid.UUID, revision int64) error {
	entry, err := s.kv.Get(ctx, id.String())
	if errors.Is(err, jetstream.ErrKeyNotFound) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("get persisted pending delivery: %w", err)
	}
	var pending PendingMessage
	if err := json.Unmarshal(entry.Value(), &pending); err != nil {
		return fmt.Errorf("decode persisted pending delivery: %w", err)
	}
	if pending.Message.Delivery.ScheduleRevision > revision {
		return nil
	}
	if err := s.kv.Delete(ctx, id.String(), jetstream.LastRevision(entry.Revision())); err != nil &&
		!errors.Is(err, jetstream.ErrKeyNotFound) && !errors.Is(err, jetstream.ErrKeyExists) {
		return fmt.Errorf("delete pending delivery: %w", err)
	}
	return nil
}
