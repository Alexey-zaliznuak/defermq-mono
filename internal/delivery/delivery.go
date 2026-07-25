package delivery

import (
	"context"
	"fmt"
	"time"

	"github.com/defermq/defermq/internal/domain"
	"github.com/google/uuid"
)

// PushRequest is the destination-independent delivery envelope. Payload bytes
// are passed through unchanged; adapters only add transport metadata.
type PushRequest struct {
	DeliveryID       uuid.UUID
	ScheduleRevision int64
	Attempt          int
	ScheduledAt      time.Time
	Destination      domain.Destination
	Payload          []byte
	ContentType      string
	Headers          map[string]string
}

type Adapter interface {
	Type() domain.DestinationType
	Push(context.Context, PushRequest) error
	Ready(context.Context) error
	Close(context.Context) error
}

type Dispatcher struct {
	adapters map[domain.DestinationType]Adapter
}

func NewDispatcher(adapters ...Adapter) (*Dispatcher, error) {
	d := &Dispatcher{adapters: make(map[domain.DestinationType]Adapter, len(adapters))}
	for _, adapter := range adapters {
		if adapter == nil {
			return nil, fmt.Errorf("register adapter: nil adapter")
		}
		typ := adapter.Type()
		if _, exists := d.adapters[typ]; exists {
			return nil, fmt.Errorf("register adapter %q: duplicate", typ)
		}
		d.adapters[typ] = adapter
	}
	return d, nil
}

func (d *Dispatcher) Push(ctx context.Context, req PushRequest) error {
	adapter, ok := d.adapters[req.Destination.Type]
	if !ok {
		return &PushError{
			Err:  fmt.Errorf("%w: %s", domain.ErrAdapterDisabled, req.Destination.Type),
			Code: "adapter_disabled",
		}
	}
	return adapter.Push(ctx, req)
}

func (d *Dispatcher) Ready(ctx context.Context) error {
	for typ, adapter := range d.adapters {
		if err := adapter.Ready(ctx); err != nil {
			return fmt.Errorf("%s adapter: %w", typ, err)
		}
	}
	return nil
}

func (d *Dispatcher) Close(ctx context.Context) error {
	var first error
	for typ, adapter := range d.adapters {
		if err := adapter.Close(ctx); err != nil && first == nil {
			first = fmt.Errorf("%s adapter: %w", typ, err)
		}
	}
	return first
}
