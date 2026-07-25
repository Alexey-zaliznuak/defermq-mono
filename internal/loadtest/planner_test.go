package loadtest

import (
	"math/rand"
	"reflect"
	"testing"
	"time"
)

func TestPlanIsDeterministicAndBounded(t *testing.T) {
	config := validTestConfig()
	config.Seed = 42
	config.Groups[0].Count = 100
	config.Groups[0].CancelFraction = 0.2
	config.Groups[0].RescheduleFraction = 0.3
	config.Groups[0].AdmissionOffset = Distribution{
		Mean: Duration(time.Second), StdDev: Duration(10 * time.Second),
		Min: Duration(500 * time.Millisecond), Max: Duration(1500 * time.Millisecond),
	}
	config.Groups[0].DeliveryDelay = Distribution{
		Mean: Duration(2 * time.Second), StdDev: Duration(10 * time.Second),
		Min: Duration(time.Second), Max: Duration(3 * time.Second),
	}
	config.Groups[0].RescheduleDelay = Distribution{
		Mean: Duration(time.Second), StdDev: Duration(10 * time.Second),
		Min: Duration(-time.Second), Max: Duration(2 * time.Second),
	}
	start := time.Date(2026, 7, 25, 12, 0, 0, 0, time.UTC)
	first := Plan(config, start)
	second := Plan(config, start)
	if !reflect.DeepEqual(first, second) {
		t.Fatal("Plan produced different output for the same seed")
	}
	actions := map[Action]int{}
	for _, message := range first {
		admission := message.AdmitAt.Sub(start)
		delay := message.DeliverAt.Sub(message.AdmitAt)
		if admission < 500*time.Millisecond || admission > 1500*time.Millisecond {
			t.Fatalf("admission offset %s is outside configured bounds", admission)
		}
		if delay < time.Second || delay > 3*time.Second {
			t.Fatalf("delivery delay %s is outside configured bounds", delay)
		}
		if message.Action == ActionReschedule {
			reschedule := message.RescheduledAt.Sub(message.DeliverAt)
			if reschedule < -time.Second || reschedule > 2*time.Second {
				t.Fatalf("reschedule delay %s is outside configured bounds", reschedule)
			}
		}
		actions[message.Action]++
	}
	for _, action := range []Action{ActionDeliver, ActionCancel, ActionReschedule} {
		if actions[action] == 0 {
			t.Fatalf("seeded plan did not select action %q", action)
		}
	}
}

func TestSampleDistributionClampsNormalDraw(t *testing.T) {
	distribution := Distribution{
		Mean: Duration(10 * time.Second), StdDev: Duration(time.Nanosecond),
		Min: Duration(time.Second), Max: Duration(2 * time.Second),
	}
	if got := sampleDistribution(rand.New(rand.NewSource(1)), distribution); got != 2*time.Second {
		t.Fatalf("sampleDistribution() = %s, want upper clamp 2s", got)
	}
}

func TestSampleUniformDistribution(t *testing.T) {
	distribution := Distribution{
		Kind: "uniform",
		Min:  Duration(10 * time.Second),
		Max:  Duration(20 * time.Second),
	}
	rng := rand.New(rand.NewSource(1))
	first := sampleDistribution(rng, distribution)
	second := sampleDistribution(rng, distribution)
	if first < 10*time.Second || first >= 20*time.Second ||
		second < 10*time.Second || second >= 20*time.Second {
		t.Fatalf("uniform samples %s and %s are outside bounds", first, second)
	}
	if first == second {
		t.Fatal("uniform samples unexpectedly match")
	}
}
