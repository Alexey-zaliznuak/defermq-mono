package delivery

import (
	"fmt"
	"math"
	"time"
)

type Jitter string

const (
	JitterNone  Jitter = "none"
	JitterFull  Jitter = "full"
	JitterEqual Jitter = "equal"
)

type Random interface {
	Float64() float64
}

type Backoff struct {
	Initial    time.Duration
	Multiplier float64
	Max        time.Duration
	Jitter     Jitter
	Random     Random
}

func (b Backoff) Validate() error {
	if b.Initial <= 0 || b.Max <= 0 || b.Initial > b.Max {
		return fmt.Errorf("invalid retry duration bounds")
	}
	if b.Multiplier < 1 || math.IsNaN(b.Multiplier) || math.IsInf(b.Multiplier, 0) {
		return fmt.Errorf("retry multiplier must be finite and at least 1")
	}
	if b.Jitter != JitterNone && b.Jitter != JitterFull && b.Jitter != JitterEqual {
		return fmt.Errorf("unsupported retry jitter %q", b.Jitter)
	}
	if b.Jitter != JitterNone && b.Random == nil {
		return fmt.Errorf("retry random source is required with jitter")
	}
	return nil
}

// Delay returns a bounded delay for a one-based attempt. retryAfter is a
// downstream minimum and is applied after jitter, still bounded by Max.
func (b Backoff) Delay(attempt int, retryAfter time.Duration) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	exponent := float64(attempt - 1)
	raw := float64(b.Initial) * math.Pow(b.Multiplier, exponent)
	if math.IsInf(raw, 0) || raw > float64(b.Max) {
		raw = float64(b.Max)
	}

	delay := time.Duration(raw)
	switch b.Jitter {
	case JitterFull:
		delay = time.Duration(float64(delay) * boundedRandom(b.Random.Float64()))
	case JitterEqual:
		half := delay / 2
		delay = half + time.Duration(float64(delay-half)*boundedRandom(b.Random.Float64()))
	}
	if retryAfter > delay {
		delay = retryAfter
	}
	if delay > b.Max {
		delay = b.Max
	}
	if delay < 0 {
		return 0
	}
	return delay
}

func boundedRandom(value float64) float64 {
	if value < 0 {
		return 0
	}
	if value >= 1 {
		return math.Nextafter(1, 0)
	}
	return value
}
