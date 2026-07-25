package natsjs

import (
	"testing"
	"time"

	"github.com/defermq/defermq/internal/domain"
	"github.com/google/uuid"
)

func TestSubjects(t *testing.T) {
	id := uuid.MustParse("018f6544-7c00-7000-8000-000000000001")
	subjects := Subjects{SchedulePrefix: "defermq.schedule.", ReadyPrefix: "defermq.ready"}
	if got, want := subjects.Schedule(id), "defermq.schedule."+id.String(); got != want {
		t.Fatalf("Schedule() = %q, want %q", got, want)
	}
	if got, want := subjects.Ready(domain.DestinationHTTP), "defermq.ready.http"; got != want {
		t.Fatalf("Ready() = %q, want %q", got, want)
	}
	if got, want := MessageID(id, 3, "schedule"), id.String()+":3:schedule"; got != want {
		t.Fatalf("MessageID() = %q, want %q", got, want)
	}
}

func TestReadyEventValidate(t *testing.T) {
	event := ReadyEvent{
		SchemaVersion:    EventSchemaVersion,
		DeliveryID:       uuid.New(),
		ScheduleRevision: 1,
		DeliverAt:        time.Now().UTC(),
		DestinationType:  domain.DestinationHTTP,
	}
	if err := event.Validate(domain.DestinationHTTP); err != nil {
		t.Fatalf("valid event rejected: %v", err)
	}
	event.SchemaVersion++
	if err := event.Validate(domain.DestinationHTTP); err == nil {
		t.Fatal("unknown schema version accepted")
	}
}
