package loadtest

import (
	"strings"
	"testing"
	"time"
)

func TestEvaluateAssertions(t *testing.T) {
	report := Report{
		CreateErrors: 2, ActionErrors: 1, Missing: 3, Duplicates: 4,
		EarlyDeliveries: 5, ExpectedDeliveries: 10, DeliveredUnique: 8,
		DeliveryLagMS: DistributionSummary{Max: 1500},
	}
	err := EvaluateAssertions(report, AssertionConfig{
		MaxCreateErrors: 1, MaxActionErrors: 0, MaxMissing: 2,
		MaxDuplicates: 3, MaxEarlyDeliveries: 4,
		MinDeliverySuccessRate: 0.9, MaxDeliveryLag: Duration(time.Second),
	})
	if err == nil {
		t.Fatal("EvaluateAssertions() unexpectedly passed")
	}
	for _, expected := range []string{
		"create errors", "action errors", "missing deliveries", "duplicates",
		"early deliveries", "success rate", "maximum delivery lag",
	} {
		if !strings.Contains(err.Error(), expected) {
			t.Errorf("error %q does not contain %q", err, expected)
		}
	}
}

func TestEvaluateAssertionsPassesDisabledThresholds(t *testing.T) {
	report := Report{ExpectedDeliveries: 1, DeliveredUnique: 1}
	if err := EvaluateAssertions(report, AssertionConfig{}); err != nil {
		t.Fatalf("EvaluateAssertions() error = %v", err)
	}
}
