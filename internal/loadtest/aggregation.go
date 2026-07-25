package loadtest

import (
	"math"
	"sort"
	"strings"
	"time"
)

const ReportSchemaVersion = 1

// RawInput is the complete, high-cardinality input consumed by Aggregate.
// Aggregate does not retain individual observations, except resource samples
// when Config.Report.IncludeSamples is enabled.
type RawInput struct {
	Config           Config
	StartedAt        time.Time
	FinishedAt       time.Time
	PlannedMessages  []PlannedMessage
	AcceptedMessages []AcceptedMessage
	Deliveries       []DeliveryObservation
	Statuses         []StatusObservation
	ResourceSamples  []ResourceSample
	Warnings         []string
}

type aggregateBucket struct {
	result              *GroupResult
	createLatencies     []float64
	deliveryLags        []float64
	receiverLags        []float64
	authoritativeTiming bool
}

// AggregateRun is a convenience adapter from Runner output to Aggregate.
func AggregateRun(config Config, result RunResult) Report {
	return Aggregate(RawInput{
		Config:           config,
		StartedAt:        result.StartedAt,
		FinishedAt:       result.FinishedAt,
		PlannedMessages:  result.Planned,
		AcceptedMessages: result.Accepted,
		Deliveries:       result.Deliveries,
		Statuses:         result.Statuses,
		ResourceSamples:  result.ResourceSamples,
		Warnings:         result.Warnings,
	})
}

// SummarizeDistribution returns a nearest-rank percentile summary. For a
// non-empty sorted sample of size n, percentile p is element ceil(p*n).
func SummarizeDistribution(values []float64) DistributionSummary {
	if len(values) == 0 {
		return DistributionSummary{}
	}
	sorted := append([]float64(nil), values...)
	sort.Float64s(sorted)
	sum := 0.0
	for _, value := range sorted {
		sum += value
	}
	percentile := func(p float64) float64 {
		index := int(math.Ceil(p*float64(len(sorted)))) - 1
		if index < 0 {
			index = 0
		}
		return sorted[index]
	}
	return DistributionSummary{
		Count: len(sorted),
		Min:   sorted[0],
		Mean:  sum / float64(len(sorted)),
		P50:   percentile(0.50),
		P95:   percentile(0.95),
		P99:   percentile(0.99),
		Max:   sorted[len(sorted)-1],
	}
}

// Aggregate creates the low-cardinality report consumed by both report
// writers. A successful accepted message has no Error and a non-empty ID.
func Aggregate(input RawInput) Report {
	report := Report{
		SchemaVersion: ReportSchemaVersion,
		Name:          input.Config.Name,
		Seed:          input.Config.Seed,
		StartedAt:     input.StartedAt,
		FinishedAt:    input.FinishedAt,
		Resources:     summarizeResources(input.ResourceSamples),
		Warnings:      append([]string(nil), input.Warnings...),
	}
	if report.FinishedAt.After(report.StartedAt) {
		report.DurationSeconds = report.FinishedAt.Sub(report.StartedAt).Seconds()
	}
	if input.Config.Report.IncludeSamples {
		report.ResourceSamples = append([]ResourceSample(nil), input.ResourceSamples...)
	}

	groupBuckets := make(map[string]*aggregateBucket)
	getBucket := func(name string) *aggregateBucket {
		bucket := groupBuckets[name]
		if bucket == nil {
			bucket = &aggregateBucket{result: &GroupResult{
				Name:               name,
				OtherFinalStatuses: make(map[string]int),
			}}
			groupBuckets[name] = bucket
		}
		return bucket
	}
	for _, group := range input.Config.Groups {
		getBucket(group.Name)
	}

	overall := aggregateBucket{result: &GroupResult{OtherFinalStatuses: make(map[string]int)}}
	acceptedByID := make(map[string]AcceptedMessage)
	expected := make(map[string]struct{})
	var firstAcceptedAt, lastAcceptedAt time.Time

	for _, planned := range input.PlannedMessages {
		report.Planned++
		getBucket(planned.Group).result.Planned++
	}
	for _, accepted := range input.AcceptedMessages {
		bucket := getBucket(accepted.Group)
		if accepted.ID != "" {
			// Preserve group ownership for delivery/status observations even
			// when a post-create cancel or reschedule operation failed.
			acceptedByID[accepted.ID] = accepted
		}
		if accepted.Error != "" || accepted.ID == "" {
			report.CreateErrors++
			bucket.result.CreateErrors++
			continue
		}
		report.Accepted++
		bucket.result.Accepted++
		if firstAcceptedAt.IsZero() || accepted.AcceptedAt.Before(firstAcceptedAt) {
			firstAcceptedAt = accepted.AcceptedAt
		}
		if accepted.AcceptedAt.After(lastAcceptedAt) {
			lastAcceptedAt = accepted.AcceptedAt
		}
		if accepted.ActionError != "" {
			report.ActionErrors++
			bucket.result.ActionErrors++
		}
		latency := durationMilliseconds(accepted.CreateLatency)
		overall.createLatencies = append(overall.createLatencies, latency)
		bucket.createLatencies = append(bucket.createLatencies, latency)
		if accepted.Action != ActionCancel || accepted.ActionError != "" {
			report.ExpectedDeliveries++
			bucket.result.ExpectedDeliveries++
			expected[accepted.ID] = struct{}{}
		}
	}

	seenDelivery := make(map[string]struct{})
	for _, delivery := range input.Deliveries {
		group := delivery.Group
		if group == "" {
			group = acceptedByID[delivery.DeliveryID].Group
		}
		bucket := getBucket(group)
		report.DeliveryAttempts++
		bucket.result.DeliveryAttempts++
		if delivery.Duplicate {
			report.Duplicates++
			bucket.result.Duplicates++
		}
		if delivery.Early {
			report.ReceiverEarly++
			bucket.result.ReceiverEarly++
		}
		if delivery.StatusCode < 200 || delivery.StatusCode >= 300 {
			continue
		}
		if _, seen := seenDelivery[delivery.DeliveryID]; seen {
			continue
		}
		seenDelivery[delivery.DeliveryID] = struct{}{}
		report.DeliveredUnique++
		bucket.result.DeliveredUnique++
		lag := durationMilliseconds(delivery.Lag)
		overall.receiverLags = append(overall.receiverLags, lag)
		bucket.receiverLags = append(bucket.receiverLags, lag)
	}

	for id := range expected {
		if _, delivered := seenDelivery[id]; delivered {
			continue
		}
		report.Missing++
		getBucket(acceptedByID[id].Group).result.Missing++
	}

	latestStatuses := make(map[string]StatusObservation)
	for _, status := range input.Statuses {
		current, exists := latestStatuses[status.DeliveryID]
		if !exists || status.ObservedAt.After(current.ObservedAt) ||
			status.ObservedAt.Equal(current.ObservedAt) {
			latestStatuses[status.DeliveryID] = status
		}
	}
	for id, status := range latestStatuses {
		group := acceptedByID[id].Group
		bucket := getBucket(group)
		switch normalizeStatus(status.Status) {
		case "delivered":
			if status.LastAttemptAt != nil && !status.DeliverAt.IsZero() {
				overall.authoritativeTiming = true
				bucket.authoritativeTiming = true
				if status.LastAttemptAt.Before(status.DeliverAt.Add(-input.Config.Load.EarlyTolerance.Value())) {
					report.EarlyDeliveries++
					bucket.result.EarlyDeliveries++
				}
			}
			if status.DeliveredAt != nil && !status.DeliverAt.IsZero() {
				lag := durationMilliseconds(status.DeliveredAt.Sub(status.DeliverAt))
				overall.deliveryLags = append(overall.deliveryLags, lag)
				bucket.deliveryLags = append(bucket.deliveryLags, lag)
			}
		case "cancelled", "canceled":
			report.Cancelled++
			bucket.result.Cancelled++
		case "dead", "dead_lettered", "dead-lettered":
			report.Dead++
			bucket.result.Dead++
		default:
			name := normalizeStatus(status.Status)
			if name == "" {
				name = "unknown"
			}
			overall.result.OtherFinalStatuses[name]++
			bucket.result.OtherFinalStatuses[name]++
		}
	}

	report.CreateLatencyMS = SummarizeDistribution(overall.createLatencies)
	if lastAcceptedAt.After(firstAcceptedAt) {
		report.AdmissionSeconds = lastAcceptedAt.Sub(firstAcceptedAt).Seconds()
		report.AdmissionRPS = float64(report.Accepted) / report.AdmissionSeconds
	}
	if !overall.authoritativeTiming {
		report.EarlyDeliveries = report.ReceiverEarly
	}
	if len(overall.deliveryLags) == 0 {
		overall.deliveryLags = overall.receiverLags
	}
	report.DeliveryLagMS = SummarizeDistribution(overall.deliveryLags)
	report.ReceiverLagMS = SummarizeDistribution(overall.receiverLags)
	if report.DurationSeconds > 0 {
		report.DeliveryThroughput = float64(report.DeliveredUnique) / report.DurationSeconds
	}

	names := make([]string, 0, len(groupBuckets))
	for name := range groupBuckets {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		bucket := groupBuckets[name]
		bucket.result.CreateLatencyMS = SummarizeDistribution(bucket.createLatencies)
		if !bucket.authoritativeTiming {
			bucket.result.EarlyDeliveries = bucket.result.ReceiverEarly
		}
		if len(bucket.deliveryLags) == 0 {
			bucket.deliveryLags = bucket.receiverLags
		}
		bucket.result.DeliveryLagMS = SummarizeDistribution(bucket.deliveryLags)
		bucket.result.ReceiverLagMS = SummarizeDistribution(bucket.receiverLags)
		if len(bucket.result.OtherFinalStatuses) == 0 {
			bucket.result.OtherFinalStatuses = nil
		}
		report.Groups = append(report.Groups, *bucket.result)
	}
	return report
}

func summarizeResources(samples []ResourceSample) map[string]ResourceSummary {
	const bytesPerMB = 1024 * 1024
	type metrics struct {
		cpu, memory, pids, rx, tx, read, write []float64
	}
	names := []string{"go", "non_go", "all"}
	collected := make(map[string]*metrics, len(names))
	for _, name := range names {
		collected[name] = &metrics{}
	}
	for _, sample := range samples {
		for _, name := range names {
			point, ok := sample.Groups[name]
			if !ok && name == "all" {
				goPoint, hasGo := sample.Groups["go"]
				nonGoPoint, hasNonGo := sample.Groups["non_go"]
				if hasGo || hasNonGo {
					point = addResourcePoints(goPoint, nonGoPoint)
					ok = true
				}
			}
			if !ok {
				continue
			}
			values := collected[name]
			values.cpu = append(values.cpu, point.CPUPercent)
			values.memory = append(values.memory, point.MemoryBytes/bytesPerMB)
			values.pids = append(values.pids, point.PIDs)
			values.rx = append(values.rx, point.NetRXBytes/bytesPerMB)
			values.tx = append(values.tx, point.NetTXBytes/bytesPerMB)
			values.read = append(values.read, point.BlockRead/bytesPerMB)
			values.write = append(values.write, point.BlockWrite/bytesPerMB)
		}
	}
	result := make(map[string]ResourceSummary, len(names))
	for _, name := range names {
		values := collected[name]
		result[name] = ResourceSummary{
			Samples:      len(values.cpu),
			CPUPercent:   SummarizeDistribution(values.cpu),
			MemoryMB:     SummarizeDistribution(values.memory),
			PIDs:         SummarizeDistribution(values.pids),
			NetRXMB:      SummarizeDistribution(values.rx),
			NetTXMB:      SummarizeDistribution(values.tx),
			BlockReadMB:  SummarizeDistribution(values.read),
			BlockWriteMB: SummarizeDistribution(values.write),
		}
	}
	return result
}

func addResourcePoints(left, right ResourcePoint) ResourcePoint {
	return ResourcePoint{
		CPUPercent:  left.CPUPercent + right.CPUPercent,
		MemoryBytes: left.MemoryBytes + right.MemoryBytes,
		PIDs:        left.PIDs + right.PIDs,
		NetRXBytes:  left.NetRXBytes + right.NetRXBytes,
		NetTXBytes:  left.NetTXBytes + right.NetTXBytes,
		BlockRead:   left.BlockRead + right.BlockRead,
		BlockWrite:  left.BlockWrite + right.BlockWrite,
	}
}

func durationMilliseconds(value time.Duration) float64 {
	return float64(value) / float64(time.Millisecond)
}

func normalizeStatus(status string) string {
	return strings.ToLower(strings.TrimSpace(status))
}
