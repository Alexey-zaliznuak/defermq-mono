package natsjs

import (
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

func TestCompatibleStreamConfigIsAdditive(t *testing.T) {
	current := jetstream.StreamConfig{
		Name:       "DEFERMQ",
		Subjects:   []string{"legacy.subject"},
		Storage:    jetstream.FileStorage,
		Retention:  jetstream.LimitsPolicy,
		MaxBytes:   2 << 30,
		MaxAge:     48 * time.Hour,
		MaxMsgSize: 128 << 10,
		Replicas:   3,
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
		t.Fatal("scheduled messages were not enabled")
	}
	if merged.MaxBytes != current.MaxBytes || merged.MaxAge != current.MaxAge ||
		merged.MaxMsgSize != current.MaxMsgSize || merged.Replicas != current.Replicas {
		t.Fatalf("existing limits were reduced: %+v", merged)
	}
	if len(merged.Subjects) != 3 {
		t.Fatalf("subjects were not merged: %v", merged.Subjects)
	}
}

func TestCompatibleStreamConfigRejectsMemory(t *testing.T) {
	current := jetstream.StreamConfig{Name: "DEFERMQ", Storage: jetstream.MemoryStorage, Retention: jetstream.LimitsPolicy}
	desired := StreamConfig{Name: "DEFERMQ", Subjects: Subjects{"a", "b"}}.Desired()
	if _, _, err := CompatibleStreamConfig(current, desired); err == nil {
		t.Fatal("memory-backed stream accepted")
	}
}
