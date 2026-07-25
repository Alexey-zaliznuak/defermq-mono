package loadtest

import (
	"math"
	"math/rand"
	"time"
)

// Plan deterministically expands message groups relative to start.
func Plan(config Config, start time.Time) []PlannedMessage {
	rng := rand.New(rand.NewSource(config.Seed))
	total := 0
	for _, group := range config.Groups {
		total += group.Count
	}
	planned := make([]PlannedMessage, 0, total)
	sequence := 0
	for _, group := range config.Groups {
		for range group.Count {
			admitAt := start.Add(sampleDistribution(rng, group.AdmissionOffset))
			deliverAt := admitAt.Add(sampleDistribution(rng, group.DeliveryDelay))
			actionRoll := rng.Float64()
			action := ActionDeliver
			if actionRoll < group.CancelFraction {
				action = ActionCancel
			} else if actionRoll < group.CancelFraction+group.RescheduleFraction {
				action = ActionReschedule
			}
			var rescheduledAt time.Time
			if action == ActionReschedule {
				rescheduledAt = deliverAt.Add(sampleDistribution(rng, group.RescheduleDelay))
			}
			planned = append(planned, PlannedMessage{
				Sequence:          sequence,
				Group:             group.Name,
				Action:            action,
				AdmitAt:           admitAt,
				DeliverAt:         deliverAt,
				RescheduledAt:     rescheduledAt,
				PayloadBytes:      group.PayloadBytes,
				MaxAttempts:       group.MaxAttempts,
				FailFirstAttempts: group.FailFirstAttempts,
				FailureStatus:     group.FailureStatus,
			})
			sequence++
		}
	}
	return planned
}

// sampleDistribution draws a normal value and clamps it to the configured
// interval. A zero bound is still a real bound when both bounds are zero.
func sampleDistribution(rng *rand.Rand, distribution Distribution) time.Duration {
	if distribution.Kind == "uniform" {
		width := distribution.Max.Value() - distribution.Min.Value()
		return distribution.Min.Value() + time.Duration(rng.Float64()*float64(width))
	}
	value := float64(distribution.Mean.Value())
	if distribution.StdDev.Value() != 0 {
		value += rng.NormFloat64() * float64(distribution.StdDev.Value())
	}
	if value < float64(distribution.Min.Value()) {
		value = float64(distribution.Min.Value())
	}
	if distribution.Max != 0 && value > float64(distribution.Max.Value()) {
		value = float64(distribution.Max.Value())
	}
	if value > float64(math.MaxInt64) {
		return time.Duration(math.MaxInt64)
	}
	if value < float64(math.MinInt64) {
		return time.Duration(math.MinInt64)
	}
	return time.Duration(math.Round(value))
}
