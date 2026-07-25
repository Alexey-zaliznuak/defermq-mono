package postgresmanager

import "testing"

func TestLoadConfigRejectsRecoveryWindowsThatCanRace(t *testing.T) {
	t.Setenv("DEFERMQ_POSTGRES_DSN", "postgres://test")

	t.Run("inflight TTL must exceed publish timeout", func(t *testing.T) {
		t.Setenv("DEFERMQ_MANAGER_SCHEDULER_INFLIGHT_TTL", "5s")
		t.Setenv("DEFERMQ_MANAGER_SCHEDULER_PUBLISH_TIMEOUT", "5s")
		if _, err := LoadConfig(); err == nil {
			t.Fatal("accepted inflight TTL equal to publish timeout")
		}
	})

	t.Run("overdue grace must exceed inflight TTL", func(t *testing.T) {
		t.Setenv("DEFERMQ_MANAGER_SCHEDULER_INFLIGHT_TTL", "10s")
		t.Setenv("DEFERMQ_MANAGER_SCHEDULER_PUBLISH_TIMEOUT", "5s")
		t.Setenv("DEFERMQ_MANAGER_OVERDUE_GRACE", "10s")
		if _, err := LoadConfig(); err == nil {
			t.Fatal("accepted overdue grace equal to inflight TTL")
		}
	})
}

func TestLoadConfigDefaultsToEightIngestWritersAcrossThirtyTwoShards(t *testing.T) {
	t.Setenv("DEFERMQ_POSTGRES_DSN", "postgres://test")
	cfg, err := LoadConfig()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.IngestWriter.WorkerCount != 8 || cfg.IngestWriter.ShardCount != 32 {
		t.Fatalf(
			"ingest workers=%d shards=%d",
			cfg.IngestWriter.WorkerCount,
			cfg.IngestWriter.ShardCount,
		)
	}
}

func TestLoadConfigRejectsMoreIngestWritersThanShards(t *testing.T) {
	t.Setenv("DEFERMQ_POSTGRES_DSN", "postgres://test")
	t.Setenv("DEFERMQ_MANAGER_INGEST_WORKERS", "33")
	t.Setenv("DEFERMQ_MANAGER_INGEST_SHARDS", "32")
	if _, err := LoadConfig(); err == nil {
		t.Fatal("accepted more ingest writers than shards")
	}
}
