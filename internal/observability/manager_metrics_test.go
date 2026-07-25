package observability

import (
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

func TestManagerLoopMetrics(t *testing.T) {
	registry := prometheus.NewRegistry()
	metrics := NewManagerMetrics(registry)

	metrics.RecordLoopError("promoter")
	metrics.ObserveLoop("promoter", 25*time.Millisecond, true, true)

	if got := gatheredValue(t, registry, "defermq_manager_loop_errors_total", "component", "promoter"); got != 1 {
		t.Fatalf("loop errors = %v, want 1", got)
	}
	if got := gatheredValue(t, registry, "defermq_manager_promoter_cycles_total", "result", "success"); got != 1 {
		t.Fatalf("successful promoter cycles = %v, want 1", got)
	}
	if got := gatheredValue(t, registry, "defermq_manager_loop_last_success_timestamp_seconds", "component", "promoter"); got <= 0 {
		t.Fatalf("last success timestamp = %v, want positive", got)
	}
	if got := gatheredValue(t, registry, "defermq_manager_loop_last_full_batch_timestamp_seconds", "component", "promoter"); got <= 0 {
		t.Fatalf("last full batch timestamp = %v, want positive", got)
	}
	if got := gatheredValue(t, registry, "defermq_manager_loop_duration_seconds", "component", "promoter"); got != 1 {
		t.Fatalf("loop duration observations = %v, want 1", got)
	}
}

func TestManagerOutboxPublishDuration(t *testing.T) {
	registry := prometheus.NewRegistry()
	metrics := NewManagerMetrics(registry)

	metrics.ObserveOutboxPublish("ready", 10*time.Millisecond)

	if got := gatheredValue(t, registry, "defermq_manager_outbox_publish_duration_seconds", "kind", "ready"); got != 1 {
		t.Fatalf("outbox publish duration observations = %v, want 1", got)
	}
}

func TestManagerNewContourMetrics(t *testing.T) {
	registry := prometheus.NewRegistry()
	metrics := NewManagerMetrics(registry)

	metrics.ObserveIngestBatch(3)
	metrics.ObserveIngestCommit(3, 10*time.Millisecond, "success")
	metrics.ObserveIngestRedelivery()
	metrics.ObserveIngestDLQ("success")
	metrics.RegistrarBatchSize.Observe(2)
	metrics.RegistrarZADD.WithLabelValues("inserted").Inc()
	metrics.SchedulerClaimed.Add(4)
	metrics.SchedulerPublished.WithLabelValues("success").Add(3)
	metrics.SchedulerReclaimed.Add(1)
	metrics.SchedulerWakeLag.Observe(25 * time.Millisecond.Seconds())
	metrics.RepairRegistrations.WithLabelValues("existing").Inc()
	metrics.ValkeyOperationErrors.WithLabelValues("claim_due").Inc()
	metrics.BucketScheduleDepth.WithLabelValues("7").Set(12)
	metrics.BucketInflightDepth.WithLabelValues("7").Set(2)

	checks := []struct {
		name, label, value string
		want               float64
	}{
		{"defermq_manager_ingest_batch_size", "", "", 1},
		{"defermq_manager_ingest_commits_total", "result", "success", 1},
		{"defermq_manager_ingest_rows_total", "result", "success", 3},
		{"defermq_manager_ingest_redeliveries_total", "", "", 1},
		{"defermq_manager_ingest_dlq_total", "result", "success", 1},
		{"defermq_manager_registrar_zadd_total", "result", "inserted", 1},
		{"defermq_manager_scheduler_claimed_total", "", "", 4},
		{"defermq_manager_scheduler_published_total", "result", "success", 3},
		{"defermq_manager_scheduler_reclaimed_total", "", "", 1},
		{"defermq_manager_repair_registrations_total", "result", "existing", 1},
		{"defermq_manager_valkey_operation_errors_total", "operation", "claim_due", 1},
		{"defermq_manager_bucket_schedule_depth", "bucket", "7", 12},
		{"defermq_manager_bucket_inflight_depth", "bucket", "7", 2},
	}
	for _, check := range checks {
		if got := gatheredValue(t, registry, check.name, check.label, check.value); got != check.want {
			t.Errorf("%s = %v, want %v", check.name, got, check.want)
		}
	}
}

func gatheredValue(t *testing.T, registry *prometheus.Registry, name, labelName, labelValue string) float64 {
	t.Helper()
	families, err := registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, family := range families {
		if family.GetName() != name {
			continue
		}
		for _, metric := range family.Metric {
			if labelName == "" {
				switch {
				case metric.Counter != nil:
					return metric.Counter.GetValue()
				case metric.Gauge != nil:
					return metric.Gauge.GetValue()
				case metric.Histogram != nil:
					return float64(metric.Histogram.GetSampleCount())
				}
			}
			for _, label := range metric.Label {
				if label.GetName() != labelName || label.GetValue() != labelValue {
					continue
				}
				switch {
				case metric.Counter != nil:
					return metric.Counter.GetValue()
				case metric.Gauge != nil:
					return metric.Gauge.GetValue()
				case metric.Histogram != nil:
					return float64(metric.Histogram.GetSampleCount())
				}
			}
		}
	}
	t.Fatalf("metric %s{%s=%q} not found", name, labelName, labelValue)
	return 0
}
