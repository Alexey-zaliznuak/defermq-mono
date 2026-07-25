package observability

import (
	"slices"
	"testing"
	"time"
)

func TestLoopHealthHonorsGraceAndSuccessfulCycles(t *testing.T) {
	now := time.Date(2026, 7, 25, 20, 0, 0, 0, time.UTC)
	health := NewLoopHealth([]string{"ingest", "scheduler"}, 30*time.Second, 20*time.Second)
	health.startedAt = now
	health.now = func() time.Time { return now }

	now = now.Add(29 * time.Second)
	if stale := health.Stale(); len(stale) != 0 {
		t.Fatalf("stale during startup grace = %v", stale)
	}
	health.Observe("ingest", true)
	health.Observe("scheduler", false)

	now = now.Add(2 * time.Second)
	if stale := health.Stale(); !slices.Equal(stale, []string{"scheduler"}) {
		t.Fatalf("stale after grace = %v", stale)
	}
	health.Observe("scheduler", true)
	if stale := health.Stale(); len(stale) != 0 {
		t.Fatalf("stale after successful cycles = %v", stale)
	}

	now = now.Add(21 * time.Second)
	if stale := health.Stale(); !slices.Equal(stale, []string{"ingest", "scheduler"}) {
		t.Fatalf("stale after threshold = %v", stale)
	}
}
