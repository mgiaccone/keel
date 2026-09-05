package ratelimit

import (
	"fmt"
	"math"
	"time"
)

// State is a key's rate-limit state: three integers whose meaning belongs to
// the [Algorithm]. Stores keep and swap it without interpreting it. The zero
// State means "never seen", and every algorithm treats it as a fresh key.
type State struct {
	A, B, C int64
}

// Algorithm is a rate-limiting rule as a pure function. Step takes a key's
// current state and the time and returns the new state and the decision; it
// must not depend on anything else, so the same algorithm gives the same
// answers against every [Store]. When a call is refused, Step should return
// the state unchanged, so the limiter can skip the write.
//
// TTL is how long an idle record must be kept before the zero State would
// give the same answers; stores expire records after it.
type Algorithm interface {
	// Name identifies the algorithm in Stats and metrics.
	Name() string
	// Validate reports a configuration that cannot be meant.
	Validate() error
	// TTL is the idle lifetime of a record.
	TTL() time.Duration
	// Step applies one call at now.
	Step(state State, now time.Time) (State, Decision)
}

// GCRA is the generic cell rate algorithm: a token bucket holding up to burst
// tokens that refills at rate per second, computed from a single timestamp,
// the theoretical arrival time of the next call. Decisions are exactly those
// of a token bucket; the single-integer state is what makes it cheap to store
// and to swap atomically. RetryAfter is the time until one token has
// refilled.
//
// State: A is the theoretical arrival time in Unix nanoseconds.
func GCRA(rate float64, burst int) Algorithm { return gcra{rate: rate, burst: burst} }

type gcra struct {
	rate  float64
	burst int
}

func (gcra) Name() string { return "gcra" }

func (g gcra) Validate() error {
	if !(g.rate > 0) || math.IsInf(g.rate, 0) {
		return fmt.Errorf("%w: GCRA: rate %v must be positive and finite", ErrInvalidOption, g.rate)
	}
	if g.burst < 1 {
		return fmt.Errorf("%w: GCRA: burst %d must be at least 1", ErrInvalidOption, g.burst)
	}
	if g.interval() < 1 {
		return fmt.Errorf("%w: GCRA: rate %v is too high to represent", ErrInvalidOption, g.rate)
	}
	return nil
}

// interval is the emission interval: the time one token takes to refill.
func (g gcra) interval() int64 { return int64(float64(time.Second) / g.rate) }

func (g gcra) TTL() time.Duration { return 2 * time.Duration(int64(g.burst)*g.interval()) }

func (g gcra) Step(s State, now time.Time) (State, Decision) {
	t := now.UnixNano()
	T := g.interval()
	tolerance := int64(g.burst-1) * T
	tat := max(s.A, t) // a TAT in the past means a full bucket
	if tat-t > tolerance {
		return s, Decision{RetryAfter: time.Duration(tat - t - tolerance)}
	}
	next := tat + T
	remaining := int((t + int64(g.burst)*T - next) / T)
	return State{A: next}, Decision{Allowed: true, Remaining: remaining}
}

// FixedWindow allows at most limit calls per key in each window, with windows
// aligned to multiples of window since the Unix epoch; the count resets at
// every boundary. It is the simplest rule to state as a quota and to reason
// about. Its known weakness is the boundary: limit calls at the end of one
// window and limit more at the start of the next is 2×limit in a span
// shorter than window. RetryAfter is the time to the next boundary.
//
// State: A is the window start in Unix nanoseconds, B the count in it.
func FixedWindow(limit int, window time.Duration) Algorithm {
	return fixedWindow{limit: limit, window: window}
}

type fixedWindow struct {
	limit  int
	window time.Duration
}

func (fixedWindow) Name() string { return "fixed_window" }

func (f fixedWindow) Validate() error {
	if f.limit < 1 {
		return fmt.Errorf("%w: FixedWindow: limit %d must be at least 1", ErrInvalidOption, f.limit)
	}
	if f.window <= 0 {
		return fmt.Errorf("%w: FixedWindow: window %s must be positive", ErrInvalidOption, f.window)
	}
	return nil
}

func (f fixedWindow) TTL() time.Duration { return 2 * f.window }

func (f fixedWindow) Step(s State, now time.Time) (State, Decision) {
	t := now.UnixNano()
	w := int64(f.window)
	start := t - t%w
	count := s.B
	if s.A != start {
		count = 0
	}
	if count >= int64(f.limit) {
		return s, Decision{RetryAfter: time.Duration(start + w - t)}
	}
	return State{A: start, B: count + 1}, Decision{Allowed: true, Remaining: f.limit - int(count) - 1}
}

// SlidingWindow allows approximately limit calls per key in any window-long
// span. It keeps the counts of the current and previous aligned windows and
// estimates the sliding count as
//
//	previous × (1 − fraction of the current window elapsed) + current
//
// which assumes the previous window's calls were evenly spread. That removes
// the fixed window's boundary burst at the cost of two counters, and is exact
// for steady traffic; under very uneven traffic the estimate can be off by a
// few percent either way. RetryAfter is the time until the estimate, with no
// new calls, would fall below limit.
//
// State: A is the current window start in Unix nanoseconds, B the count in
// it, C the count in the previous window.
func SlidingWindow(limit int, window time.Duration) Algorithm {
	return slidingWindow{limit: limit, window: window}
}

type slidingWindow struct {
	limit  int
	window time.Duration
}

func (slidingWindow) Name() string { return "sliding_window" }

func (s slidingWindow) Validate() error {
	if s.limit < 1 {
		return fmt.Errorf("%w: SlidingWindow: limit %d must be at least 1", ErrInvalidOption, s.limit)
	}
	if s.window <= 0 {
		return fmt.Errorf("%w: SlidingWindow: window %s must be positive", ErrInvalidOption, s.window)
	}
	return nil
}

func (s slidingWindow) TTL() time.Duration { return 2 * s.window }

func (s slidingWindow) Step(st State, now time.Time) (State, Decision) {
	t := now.UnixNano()
	w := int64(s.window)
	start := t - t%w
	cur, prev := st.B, st.C
	switch {
	case st.A == start:
	case st.A == start-w:
		prev, cur = cur, 0
	default: // more than one window has passed: nothing recent remains
		prev, cur = 0, 0
	}
	frac := float64(t-start) / float64(w)
	est := float64(prev)*(1-frac) + float64(cur)
	limit := float64(s.limit)
	if est+1 <= limit {
		return State{A: start, B: cur + 1, C: prev}, Decision{Allowed: true, Remaining: int(limit - est - 1)}
	}
	remainingInWindow := time.Duration(start + w - t)
	var wait time.Duration
	if float64(cur)+1 <= limit {
		// Enough of previous must decay: previous×(1−f') ≤ limit−1−current.
		need := 1 - (limit-1-float64(cur))/float64(prev)
		wait = time.Duration((need - frac) * float64(w))
	} else {
		// current alone exceeds the limit: wait for the boundary, then for
		// current, as the new previous, to decay: current×(1−f) ≤ limit−1.
		f := 1 - (limit-1)/float64(cur)
		wait = remainingInWindow + time.Duration(f*float64(w))
	}
	return st, Decision{RetryAfter: time.Duration(math.Ceil(float64(wait)))}
}
