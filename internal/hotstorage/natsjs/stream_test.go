package natsjs

import (
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

func TestCompatibleStreamConfigRemovesScheduledSubjects(t *testing.T) {
	current := jetstream.StreamConfig{
		Name:              "DEFERMQ",
		Subjects:          []string{"legacy.subject"},
		Storage:           jetstream.FileStorage,
		Retention:         jetstream.LimitsPolicy,
		MaxBytes:          2 << 30,
		MaxAge:            48 * time.Hour,
		MaxMsgSize:        128 << 10,
		Replicas:          3,
		AllowMsgSchedules: true,
	}
	desired := StreamConfig{
		Name:            "DEFERMQ",
		Subjects:        Subjects{"defermq.schedule", "defermq.ready"},
		MaxBytes:        1 << 30,
		MaxAge:          24 * time.Hour,
		MaxMsgSize:      64 << 10,
		Replicas:        1,
		DuplicateWindow: 10 * time.Minute,
	}.Desired()
	merged, changed, err := CompatibleStreamConfig(current, desired)
	if err != nil {
		t.Fatal(err)
	}
	if !changed || !merged.AllowMsgSchedules {
		t.Fatal("existing schedule capability must be preserved for an upgrade-safe stream update")
	}
	if merged.MaxBytes != current.MaxBytes || merged.MaxAge != current.MaxAge ||
		merged.MaxMsgSize != current.MaxMsgSize || merged.Replicas != current.Replicas {
		t.Fatalf("existing limits were reduced: %+v", merged)
	}
	if len(merged.Subjects) != 1 || merged.Subjects[0] != "defermq.ready.*" {
		t.Fatalf("stream does not contain only ready subjects: %v", merged.Subjects)
	}
}

func TestCompatibleStreamConfigRejectsMemory(t *testing.T) {
	current := jetstream.StreamConfig{Name: "DEFERMQ", Storage: jetstream.MemoryStorage, Retention: jetstream.LimitsPolicy}
	desired := StreamConfig{Name: "DEFERMQ", Subjects: Subjects{"a", "b"}}.Desired()
	if _, _, err := CompatibleStreamConfig(current, desired); err == nil {
		t.Fatal("memory-backed stream accepted")
	}
}
