package overload

import (
	"fmt"
	"math"
	"time"
)

// State is a limiter's algorithm state: three integers whose meaning belongs
// to the [Algorithm]. The [Limiter] keeps and hands back the value without
// interpreting it. The zero State means "not started yet", and every algorithm
// must treat it as a request for its initial capacity, ignoring the [Signal]:
// [New] steps a fresh state exactly once, before any request has run, to learn
// what capacity to open with.
type State struct {
	A, B, C int64
}

// Signal is what one completed request tells the algorithm.
type Signal struct {
	// Waited reports whether the request queued before it was admitted, which
	// only happens under [WithMaxWait].
	Waited bool
	// Sojourn is how long the request waited to be admitted; zero, and
	// meaningless, unless Waited. No algorithm in this package reads it — the
	// queue's own give-up rule owns waiting — and one that does couples its
	// capacity estimate to a queue that may not exist.
	Sojourn time.Duration
	// Duration is the request's own service time: admission to release, with
	// any queue wait excluded. That exclusion is what makes an algorithm read
	// the same numbers with and without [WithMaxWait]; measuring the wait here
	// instead would make queueing look like slowness and shrink capacity for
	// having queued.
	Duration time.Duration
	// Err is the outcome release was called with; nil on success. Treat a
	// non-nil Err as the strongest evidence of overload there is, not as a
	// latency sample: a request that failed fast is fast for the wrong reason.
	Err error
}

// Decision is the algorithm's answer after one [Signal].
type Decision struct {
	// Capacity is how many requests may be in flight at once, across every
	// priority. The shares given to [New] divide it; [Critical] gets all of
	// it. A Capacity below 1 refuses every request including Critical, and can
	// then only recover from a request already in flight releasing — neither
	// algorithm here ever returns one, and a third-party algorithm that does
	// is choosing a state it may not be able to leave.
	Capacity int
	// ShedBelow refuses priorities strictly below it until the next Step,
	// whatever the shares would otherwise allow. It is how an algorithm says
	// "capacity is not the problem any more, the work is": both algorithms
	// here raise it to [Default] once they have backed all the way down to
	// their floor. Leave it at [Sheddable] to shed nothing on this account.
	ShedBelow Priority
}

// Algorithm computes capacity from completed requests, as a pure function:
// Step takes the state it last returned, one request's [Signal] and the time,
// and returns the new state and the [Decision]. It must depend on nothing
// else — no clock of its own, no randomness, no accumulated state outside
// State — so a limiter's whole capacity trajectory is reproducible from the
// signals it saw.
//
// One value is shared by every [Limiter] it is given to, so an implementation
// must be safe for concurrent use, which for a pure function it is by
// construction. Keeping mutable state on the receiver instead is the mistake
// this shape exists to prevent.
type Algorithm interface {
	// Name identifies the algorithm in [Stats] and metrics.
	Name() string
	// Validate reports a configuration that cannot be meant.
	Validate() error
	// Step applies one completed request at now.
	Step(state State, sig Signal, now time.Time) (State, Decision)
}

const (
	// Capacity is held scaled inside State so that a smoothed step smaller
	// than one slot still moves it; rounding every step to a whole number
	// would quantise the smoothing away.
	_scale = 1000
	// How much of each step's computed target is taken, the rest being the
	// current capacity. Low enough that one outlier request cannot move the
	// limit far, high enough to follow a real shift within a few requests.
	_smoothing = 0.2
	// The most one step may shrink capacity by, as a fraction. Without a floor
	// a single request an order of magnitude slower than the baseline would
	// collapse the limit in one step.
	_gradientFloor = 0.5
	// How many samples the baseline takes to follow a genuine, sustained rise
	// in service time. A new best replaces it at once; only the drift upward
	// is slow, so a service that has actually got slower is not held to the
	// latency of a lucky request forever.
	_baselineDecay = 100
)

// Gradient is the recommended algorithm: it calibrates itself, with no target
// latency to guess. It tracks the best service time it has itself observed as
// a moving baseline and moves capacity as current service time drifts from
// that baseline — a request twice the baseline is the queue building, wherever
// the baseline happens to be — so it works on a 2ms endpoint and a 2s one
// without being told which it is. Capacity starts at max and moves within
// [min, max]; once it has backed down to min the algorithm also refuses
// [Sheddable] outright, via [Decision.ShedBelow].
//
// The failure mode to know about is the one every self-calibrating limiter
// has: the baseline is only as good as the best request it has seen, so a
// service that has been degraded since the process started calibrates to
// degraded and calls it normal. It recalibrates as soon as one genuinely fast
// request lands, which in practice is the first minute after the dependency
// recovers. Pair it with a breaker, which judges outcomes rather than drift,
// if that window matters. Use [AIMD] instead when you do know your target
// latency and want a rule you can predict by hand.
//
// State: A is capacity scaled by 1000, B the baseline in nanoseconds, C the
// number of samples taken.
func Gradient(min, max int) Algorithm { return gradient{min: min, max: max} }

type gradient struct {
	min, max int
}

func (gradient) Name() string { return "gradient" }

func (g gradient) Validate() error {
	if g.min < 1 {
		return fmt.Errorf("%w: Gradient: min %d must be at least 1", ErrInvalidOption, g.min)
	}
	if g.max < g.min {
		return fmt.Errorf("%w: Gradient: max %d must be at least min %d", ErrInvalidOption, g.max, g.min)
	}
	return nil
}

func (g gradient) Step(s State, sig Signal, _ time.Time) (State, Decision) {
	if s.A == 0 {
		s = State{A: int64(g.max) * _scale}
		return s, g.decide(s.A)
	}
	if sig.Duration <= 0 {
		return s, g.decide(s.A) // nothing was measured; nothing to learn from
	}

	d := int64(sig.Duration)
	if s.C == 0 || d < s.B {
		s.B = d
	} else {
		s.B += (d - s.B) / _baselineDecay
	}
	s.C++

	switch {
	case sig.Err != nil:
		s.A = max(s.A/2, int64(g.min)*_scale)
	case s.C == 1:
		// One sample is a baseline, not yet a gradient: there is nothing to
		// compare it against but itself.
	default:
		limit := float64(s.A) / _scale
		grad := max(_gradientFloor, min(1, float64(s.B)/float64(d)))
		// The sqrt term is the headroom probe: without it capacity could only
		// ever shrink, since grad is at most 1 and a limiter that never tries
		// a larger limit never discovers the load has passed.
		target := limit*grad + math.Sqrt(limit)
		next := limit*(1-_smoothing) + target*_smoothing
		s.A = min(max(int64(math.Round(next*_scale)), int64(g.min)*_scale), int64(g.max)*_scale)
	}
	return s, g.decide(s.A)
}

func (g gradient) decide(a int64) Decision {
	c := int(a / _scale)
	d := Decision{Capacity: c}
	if c <= g.min && g.max > g.min {
		d.ShedBelow = Default
	}
	return d
}

// AIMD moves capacity between min and max by additive increase, multiplicative
// decrease against an explicit target service time: every request within
// target raises the limit by one, and a run of requests slower than target, or
// failing, halves it. It is the rule breaker.WithAdaptiveInFlight uses on the
// outbound side, and the one to reach for when you know your target latency
// and want a limit you can predict by hand rather than [Gradient]'s estimate.
//
// interval is hysteresis, and it is the parameter that makes this usable
// inbound: the halving fires only once requests have been bad continuously for
// a whole interval, never on one slow request, and one good request ends the
// run. Without it a low-volume path collapses its own limit on a single
// unlucky query — the jitter docs/breaker.md warns about for the outbound
// AIMD. Size target at about your p99 in good health and interval at several
// times your p99, long enough that a burst of legitimately slow work rides
// through it. Once capacity has reached min, [Sheddable] is refused outright
// via [Decision.ShedBelow].
//
// State: A is capacity, B the start of the current run of bad requests in Unix
// nanoseconds, C non-zero while such a run is open.
func AIMD(min, max int, target, interval time.Duration) Algorithm {
	return aimd{min: min, max: max, target: target, interval: interval}
}

type aimd struct {
	min, max int
	target   time.Duration
	interval time.Duration
}

func (aimd) Name() string { return "aimd" }

func (a aimd) Validate() error {
	if a.min < 1 {
		return fmt.Errorf("%w: AIMD: min %d must be at least 1", ErrInvalidOption, a.min)
	}
	if a.max < a.min {
		return fmt.Errorf("%w: AIMD: max %d must be at least min %d", ErrInvalidOption, a.max, a.min)
	}
	if a.target <= 0 {
		return fmt.Errorf("%w: AIMD: target %s must be positive", ErrInvalidOption, a.target)
	}
	if a.interval <= 0 {
		return fmt.Errorf("%w: AIMD: interval %s must be positive", ErrInvalidOption, a.interval)
	}
	return nil
}

func (a aimd) Step(s State, sig Signal, now time.Time) (State, Decision) {
	if s.A == 0 {
		s = State{A: int64(a.max)}
		return s, a.decide(s.A)
	}
	if sig.Duration <= 0 && sig.Err == nil {
		// Nothing was measured and nothing failed: a clock too coarse to
		// separate admission from release says nothing about capacity, and
		// must not be read as a request that missed the target.
		return s, a.decide(s.A)
	}

	if sig.Err == nil && sig.Duration <= a.target {
		s.A = min(s.A+1, int64(a.max))
		s.B, s.C = 0, 0
		return s, a.decide(s.A)
	}

	t := now.UnixNano()
	if s.C == 0 {
		s.B, s.C = t, 1
		return s, a.decide(s.A)
	}
	if t-s.B >= int64(a.interval) {
		// Restart the run rather than clearing it: capacity keeps halving
		// while things stay bad, but never faster than once per interval.
		s.A, s.B = max(s.A/2, int64(a.min)), t
	}
	return s, a.decide(s.A)
}

func (a aimd) decide(c int64) Decision {
	d := Decision{Capacity: int(c)}
	if int(c) <= a.min && a.max > a.min {
		d.ShedBelow = Default
	}
	return d
}
