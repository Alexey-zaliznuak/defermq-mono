package natsjs

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/defermq/defermq/internal/domain"
	"github.com/google/uuid"
)

const EventSchemaVersion = 1

type ReadyEvent struct {
	SchemaVersion    int                    `json:"schema_version"`
	DeliveryID       uuid.UUID              `json:"delivery_id"`
	ScheduleRevision int64                  `json:"schedule_revision"`
	DeliverAt        time.Time              `json:"deliver_at"`
	DestinationType  domain.DestinationType `json:"destination_type"`
}

func (e ReadyEvent) Validate(expected domain.DestinationType) error {
	if e.SchemaVersion != EventSchemaVersion {
		return fmt.Errorf("unsupported schema version %d", e.SchemaVersion)
	}
	if e.DeliveryID == uuid.Nil || e.ScheduleRevision <= 0 || e.DeliverAt.IsZero() {
		return errors.New("invalid ready event")
	}
	if e.DestinationType != expected {
		return errors.New("destination type does not match consumer")
	}
	return nil
}

type Subjects struct {
	SchedulePrefix string
	ReadyPrefix    string
}

func (s Subjects) Schedule(deliveryID uuid.UUID) string {
	return strings.TrimSuffix(s.SchedulePrefix, ".") + "." + deliveryID.String()
}

func (s Subjects) Ready(destinationType domain.DestinationType) string {
	return strings.TrimSuffix(s.ReadyPrefix, ".") + "." + string(destinationType)
}

func (s Subjects) ScheduleWildcard() string { return strings.TrimSuffix(s.SchedulePrefix, ".") + ".*" }
func (s Subjects) ReadyWildcard() string    { return strings.TrimSuffix(s.ReadyPrefix, ".") + ".*" }

func MessageID(deliveryID uuid.UUID, revision int64, kind string) string {
	return fmt.Sprintf("%s:%d:%s", deliveryID, revision, kind)
}
