package loadtest

import (
	"errors"
	"fmt"
)

func EvaluateAssertions(report Report, config AssertionConfig) error {
	var failures []error
	if report.CreateErrors > config.MaxCreateErrors {
		failures = append(failures, fmt.Errorf("create errors %d exceed limit %d", report.CreateErrors, config.MaxCreateErrors))
	}
	if report.ActionErrors > config.MaxActionErrors {
		failures = append(failures, fmt.Errorf("action errors %d exceed limit %d", report.ActionErrors, config.MaxActionErrors))
	}
	if report.Missing > config.MaxMissing {
		failures = append(failures, fmt.Errorf("missing deliveries %d exceed limit %d", report.Missing, config.MaxMissing))
	}
	if report.Duplicates > config.MaxDuplicates {
		failures = append(failures, fmt.Errorf("duplicates %d exceed limit %d", report.Duplicates, config.MaxDuplicates))
	}
	if report.EarlyDeliveries > config.MaxEarlyDeliveries {
		failures = append(failures, fmt.Errorf("early deliveries %d exceed limit %d", report.EarlyDeliveries, config.MaxEarlyDeliveries))
	}
	successRate := 1.0
	if report.ExpectedDeliveries > 0 {
		successRate = float64(report.DeliveredUnique) / float64(report.ExpectedDeliveries)
	}
	if successRate < config.MinDeliverySuccessRate {
		failures = append(failures, fmt.Errorf(
			"delivery success rate %.6f is below minimum %.6f",
			successRate, config.MinDeliverySuccessRate,
		))
	}
	if limit := config.MaxDeliveryLag.Value(); limit > 0 && report.DeliveryLagMS.Max > float64(limit.Milliseconds()) {
		failures = append(failures, fmt.Errorf(
			"maximum delivery lag %.3fms exceeds limit %s",
			report.DeliveryLagMS.Max, limit,
		))
	}
	return errors.Join(failures...)
}
