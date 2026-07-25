package loadtest

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestSummarizeDistributionNearestRank(t *testing.T) {
	t.Parallel()

	got := SummarizeDistribution([]float64{100, 1, 3, 2, 4})
	want := DistributionSummary{
		Count: 5, Min: 1, Mean: 22, P50: 3, P95: 100, P99: 100, Max: 100,
	}
	if got != want {
		t.Fatalf("summary = %#v, want %#v", got, want)
	}

	if empty := SummarizeDistribution(nil); empty != (DistributionSummary{}) {
		t.Fatalf("empty summary = %#v, want zero value", empty)
	}
}

func TestAggregateResultsAndDuplicateCounting(t *testing.T) {
	t.Parallel()

	started := time.Date(2026, 7, 25, 12, 0, 0, 0, time.UTC)
	input := RawInput{
		Config: Config{
			Name: "test", Seed: 42,
			Groups: []MessageGroup{{Name: "alpha"}, {Name: "beta"}},
		},
		StartedAt:  started,
		FinishedAt: started.Add(2 * time.Second),
		PlannedMessages: []PlannedMessage{
			{Sequence: 1, Group: "alpha", Action: ActionDeliver},
			{Sequence: 2, Group: "alpha", Action: ActionCancel},
			{Sequence: 3, Group: "alpha", Action: ActionDeliver},
			{Sequence: 4, Group: "beta", Action: ActionReschedule},
			{Sequence: 5, Group: "beta", Action: ActionDeliver},
		},
		AcceptedMessages: []AcceptedMessage{
			{PlannedMessage: PlannedMessage{Group: "alpha", Action: ActionDeliver}, ID: "delivery-a", CreateLatency: 10 * time.Millisecond},
			{PlannedMessage: PlannedMessage{Group: "alpha", Action: ActionCancel}, ID: "delivery-b", CreateLatency: 20 * time.Millisecond},
			{PlannedMessage: PlannedMessage{Group: "alpha", Action: ActionDeliver}, Error: "rejected"},
			{PlannedMessage: PlannedMessage{Group: "beta", Action: ActionReschedule}, ID: "delivery-c", CreateLatency: 30 * time.Millisecond},
			{PlannedMessage: PlannedMessage{Group: "beta", Action: ActionDeliver}, ID: "delivery-d", CreateLatency: 40 * time.Millisecond},
		},
		Deliveries: []DeliveryObservation{
			{DeliveryID: "delivery-a", Group: "alpha", Lag: 100 * time.Millisecond, StatusCode: 204},
			{DeliveryID: "delivery-a", Group: "alpha", Lag: 999 * time.Millisecond, StatusCode: 204, Duplicate: true, Early: true},
			{DeliveryID: "delivery-c", Group: "beta", Lag: 200 * time.Millisecond, StatusCode: 204},
		},
		Statuses: []StatusObservation{
			{DeliveryID: "delivery-b", Status: "cancelled", ObservedAt: started},
			{DeliveryID: "delivery-c", Status: "dead", ObservedAt: started},
			{DeliveryID: "delivery-d", Status: "scheduled", ObservedAt: started},
			{DeliveryID: "delivery-d", Status: "pending", ObservedAt: started.Add(time.Second)},
		},
	}

	report := Aggregate(input)
	if report.Planned != 5 || report.Accepted != 4 || report.CreateErrors != 1 {
		t.Fatalf("create totals = planned %d, accepted %d, errors %d", report.Planned, report.Accepted, report.CreateErrors)
	}
	if report.ExpectedDeliveries != 3 || report.DeliveredUnique != 2 ||
		report.DeliveryAttempts != 3 || report.Duplicates != 1 || report.Missing != 1 {
		t.Fatalf("delivery totals are wrong: %#v", report)
	}
	if report.EarlyDeliveries != 1 || report.Cancelled != 1 || report.Dead != 1 {
		t.Fatalf("final totals are wrong: early=%d cancelled=%d dead=%d",
			report.EarlyDeliveries, report.Cancelled, report.Dead)
	}
	if report.DeliveryThroughput != 1 {
		t.Fatalf("throughput = %v, want 1", report.DeliveryThroughput)
	}
	if report.CreateLatencyMS != (DistributionSummary{
		Count: 4, Min: 10, Mean: 25, P50: 20, P95: 40, P99: 40, Max: 40,
	}) {
		t.Fatalf("create latency = %#v", report.CreateLatencyMS)
	}
	if report.DeliveryLagMS != (DistributionSummary{
		Count: 2, Min: 100, Mean: 150, P50: 100, P95: 200, P99: 200, Max: 200,
	}) {
		t.Fatalf("delivery lag = %#v; duplicate must not affect it", report.DeliveryLagMS)
	}
	if len(report.Groups) != 2 || report.Groups[0].Name != "alpha" || report.Groups[1].Name != "beta" {
		t.Fatalf("groups are not deterministic: %#v", report.Groups)
	}
	if report.Groups[1].OtherFinalStatuses["pending"] != 1 {
		t.Fatalf("latest other status not aggregated: %#v", report.Groups[1].OtherFinalStatuses)
	}
}

func TestAggregateResourceSummariesAndSampleFlag(t *testing.T) {
	t.Parallel()

	const mb = 1024 * 1024
	samples := []ResourceSample{
		{Groups: map[string]ResourcePoint{
			"go":     {CPUPercent: 10, MemoryBytes: 100 * mb, PIDs: 2, NetRXBytes: mb},
			"non_go": {CPUPercent: 20, MemoryBytes: 200 * mb, PIDs: 3, NetRXBytes: 2 * mb},
		}},
		{Groups: map[string]ResourcePoint{
			"go":     {CPUPercent: 30, MemoryBytes: 300 * mb, PIDs: 4, NetRXBytes: 4 * mb},
			"non_go": {CPUPercent: 40, MemoryBytes: 400 * mb, PIDs: 5, NetRXBytes: 8 * mb},
		}},
	}
	withoutSamples := Aggregate(RawInput{ResourceSamples: samples})
	if withoutSamples.ResourceSamples != nil {
		t.Fatal("resource samples included while flag is false")
	}
	if got := withoutSamples.Resources["go"]; got.Samples != 2 ||
		got.CPUPercent.Mean != 20 || got.CPUPercent.P95 != 30 ||
		got.MemoryMB.Max != 300 || got.NetRXMB.Max != 4 {
		t.Fatalf("go resource summary = %#v", got)
	}
	if got := withoutSamples.Resources["all"]; got.Samples != 2 ||
		got.CPUPercent.Mean != 50 || got.MemoryMB.Max != 700 || got.PIDs.Max != 9 {
		t.Fatalf("derived all resource summary = %#v", got)
	}

	withSamples := Aggregate(RawInput{
		Config:          Config{Report: ReportConfig{IncludeSamples: true}},
		ResourceSamples: samples,
	})
	if len(withSamples.ResourceSamples) != 2 {
		t.Fatalf("included samples = %d, want 2", len(withSamples.ResourceSamples))
	}
}

func TestReportWritersContentsAndOverwrite(t *testing.T) {
	t.Parallel()

	directory := t.TempDir()
	report := Report{
		SchemaVersion: 1,
		Name:          "example | run",
		StartedAt:     time.Date(2026, 7, 25, 12, 0, 0, 0, time.UTC),
		FinishedAt:    time.Date(2026, 7, 25, 12, 0, 2, 0, time.UTC),
		Planned:       2, Accepted: 1, CreateErrors: 1,
		ExpectedDeliveries: 1, DeliveredUnique: 1, DeliveryAttempts: 2,
		Duplicates: 1, DeliveryThroughput: 0.5,
		CreateLatencyMS: DistributionSummary{Count: 1, Min: 5, Mean: 5, P50: 5, P95: 5, P99: 5, Max: 5},
		Groups: []GroupResult{{
			Name: "alpha|one", Planned: 2, Accepted: 1, CreateErrors: 1,
			ExpectedDeliveries: 1, DeliveredUnique: 1, DeliveryAttempts: 2,
			Duplicates: 1, OtherFinalStatuses: map[string]int{"pending": 1},
		}},
		Resources: map[string]ResourceSummary{
			"go":     {Samples: 1, CPUPercent: DistributionSummary{Mean: 10, P95: 10, P99: 10, Max: 10}},
			"non_go": {},
			"all":    {Samples: 1, CPUPercent: DistributionSummary{Mean: 10, P95: 10, P99: 10, Max: 10}},
		},
		Warnings: []string{"warning\nwithout a new row"},
	}
	config := ReportConfig{
		Directory: directory, JSONFile: "report.json", MarkdownFile: "report.md",
	}
	if err := WriteReports(report, config); err != nil {
		t.Fatal(err)
	}
	report.Name = "overwritten"
	if err := WriteReports(report, config); err != nil {
		t.Fatalf("atomic overwrite: %v", err)
	}

	jsonData, err := os.ReadFile(filepath.Join(directory, "report.json"))
	if err != nil {
		t.Fatal(err)
	}
	var decoded Report
	if err := json.Unmarshal(jsonData, &decoded); err != nil {
		t.Fatalf("invalid JSON: %v", err)
	}
	if decoded.Name != "overwritten" || !bytes.HasSuffix(jsonData, []byte("\n")) {
		t.Fatalf("unexpected JSON report: %s", jsonData)
	}

	markdownData, err := os.ReadFile(filepath.Join(directory, "report.md"))
	if err != nil {
		t.Fatal(err)
	}
	markdown := string(markdownData)
	for _, expected := range []string{
		"# Load test report: overwritten",
		"Percentiles use the nearest-rank method.",
		"| 2 | 1 | 1 | 0 | 1 | 1 | 2 | 1 |",
		"| alpha\\|one | 2 | 1 | 1 | 0 | 1 | 1 | 2 | 1 |",
		"| go | 1 | 10.000 | 10.000 | 10.000 | 10.000 |",
		"- warning without a new row",
	} {
		if !strings.Contains(markdown, expected) {
			t.Errorf("Markdown report missing %q:\n%s", expected, markdown)
		}
	}
	if strings.Contains(markdown, "delivery-a") || strings.Contains(markdown, "resource_samples") {
		t.Fatalf("Markdown contains high-cardinality data:\n%s", markdown)
	}
}
