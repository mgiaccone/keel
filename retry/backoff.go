package retry

import (
	"fmt"
	"time"
)

// Backoff is a retry schedule as a pure function. Delay returns the wait
// before retry n, counting from 1 for the first retry, given the previous
// wait, 0 before the first, and u, a uniform draw in [0, 1) that the retrier
// supplies from its own seeded generator. Under [WithHedge] n is the number
// of attempts that have failed so far, hedges included. A Backoff must not
// keep state or read a clock, so the same schedule gives the same waits
// everywhere and is tested with explicit inputs.
//
// Every schedule except [Constant] is jittered. Unjittered retries from a
// fleet that failed together arrive together, and a dependency that is
// recovering fails again under the synchronised burst.
type Backoff interface {
	// Name identifies the schedule in Stats and metrics.
	Name() string
	// Validate reports a configuration that cannot be meant.
	Validate() error
	// Delay is the wait before retry n.
	Delay(retry int, previous time.Duration, u float64) time.Duration
}

// Constant waits d before every retry, with no jitter. It suits a local
// pacing decision, such as retrying after a limiter refusal whose
// [Delayed] error already says when, and tests. It is the wrong choice for a
// fleet retrying a shared dependency: every instance retries in lockstep.
func Constant(d time.Duration) Backoff { return constant{d: d} }

type constant struct{ d time.Duration }

func (constant) Name() string { return "constant" }

func (c constant) Validate() error {
	if c.d < 0 {
		return fmt.Errorf("%w: Constant(%s): must not be negative", ErrInvalidOption, c.d)
	}
	return nil
}

func (c constant) Delay(int, time.Duration, float64) time.Duration { return c.d }

// Exponential doubles from base per retry, capped at max, and applies full
// jitter: the wait is drawn uniformly from [0, that). It is the general
// default. The lower bound of zero is deliberate: full jitter spreads a fleet
// better than any scheme that keeps a floor, at the cost of the occasional
// near-immediate retry.
func Exponential(base, max time.Duration) Backoff { return exponential{base: base, max: max} }

type exponential struct{ base, max time.Duration }

func (exponential) Name() string { return "exponential" }

func (e exponential) Validate() error { return validateRange("Exponential", e.base, e.max) }

func (e exponential) Delay(retry int, _ time.Duration, u float64) time.Duration {
	return jitter(doubled(e.base, e.max, retry), u)
}

// Decorrelated is the decorrelated jitter schedule: the wait is drawn
// uniformly from [base, 3 × previous), capped at max, with the first draw
// from [base, 3 × base). Each wait depends on the last, so a fleet that
// started in lockstep drifts apart faster than under [Exponential], and the
// floor of base guarantees some pause. Prefer it for a busy fleet against one
// dependency.
func Decorrelated(base, max time.Duration) Backoff { return decorrelated{base: base, max: max} }

type decorrelated struct{ base, max time.Duration }

func (decorrelated) Name() string { return "decorrelated" }

func (d decorrelated) Validate() error { return validateRange("Decorrelated", d.base, d.max) }

func (d decorrelated) Delay(_ int, previous time.Duration, u float64) time.Duration {
	if previous < d.base {
		previous = d.base
	}
	upper := d.max
	if previous < d.max/3 {
		upper = 3 * previous
	}
	if upper <= d.base {
		return d.base
	}
	return min(d.base+time.Duration(float64(upper-d.base)*u), d.max)
}

// Fibonacci grows base by the Fibonacci sequence per retry (1, 1, 2, 3, 5…),
// capped at max, with full jitter like [Exponential]. It climbs more gently,
// for a dependency that recovers on its own timescale rather than one that
// needs the pressure taken off quickly.
func Fibonacci(base, max time.Duration) Backoff { return fibonacci{base: base, max: max} }

type fibonacci struct{ base, max time.Duration }

func (fibonacci) Name() string { return "fibonacci" }

func (f fibonacci) Validate() error { return validateRange("Fibonacci", f.base, f.max) }

func (f fibonacci) Delay(retry int, _ time.Duration, u float64) time.Duration {
	a, b := f.base, f.base
	for range retry - 1 {
		if b >= f.max-a { // the next term would pass max, or overflow: saturate
			a, b = b, f.max
		} else {
			a, b = b, a+b
		}
	}
	return jitter(min(a, f.max), u)
}

func validateRange(name string, base, max time.Duration) error {
	if base <= 0 || max < base {
		return fmt.Errorf("%w: %s(%s, %s): need 0 < base <= max", ErrInvalidOption, name, base, max)
	}
	return nil
}

// doubled is base × 2^(retry−1) saturating at max, without overflow.
func doubled(base, max time.Duration, retry int) time.Duration {
	d := base
	for range retry - 1 {
		if d >= max/2 {
			return max
		}
		d *= 2
	}
	return min(d, max)
}

// jitter is full jitter: a uniform draw from [0, d).
func jitter(d time.Duration, u float64) time.Duration {
	return time.Duration(float64(d) * u)
}
