package delivery

import (
	"testing"
	"time"
)

type fixedRandom float64

func (r fixedRandom) Float64() float64 { return float64(r) }

func TestBackoffDelay(t *testing.T) {
	tests := []struct {
		name       string
		backoff    Backoff
		attempt    int
		retryAfter time.Duration
		want       time.Duration
	}{
		{
			name:    "exponential",
			backoff: Backoff{Initial: time.Second, Multiplier: 2, Max: time.Minute, Jitter: JitterNone},
			attempt: 4,
			want:    8 * time.Second,
		},
		{
			name:    "bounded",
			backoff: Backoff{Initial: time.Second, Multiplier: 10, Max: 15 * time.Second, Jitter: JitterNone},
			attempt: 10,
			want:    15 * time.Second,
		},
		{
			name:    "full jitter",
			backoff: Backoff{Initial: 10 * time.Second, Multiplier: 2, Max: time.Minute, Jitter: JitterFull, Random: fixedRandom(0.25)},
			attempt: 1,
			want:    2500 * time.Millisecond,
		},
		{
			name:       "retry after wins but is bounded",
			backoff:    Backoff{Initial: time.Second, Multiplier: 2, Max: 10 * time.Second, Jitter: JitterNone},
			attempt:    1,
			retryAfter: time.Minute,
			want:       10 * time.Second,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := test.backoff.Delay(test.attempt, test.retryAfter); got != test.want {
				t.Fatalf("Delay() = %s, want %s", got, test.want)
			}
		})
	}
}

func TestBackoffValidateRequiresRandomForJitter(t *testing.T) {
	backoff := Backoff{Initial: time.Second, Multiplier: 2, Max: time.Minute, Jitter: JitterFull}
	if err := backoff.Validate(); err == nil {
		t.Fatal("Validate() unexpectedly succeeded")
	}
}
