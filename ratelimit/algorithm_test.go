package ratelimit

import (
	"errors"
	"math"
	"math/rand/v2"
	"testing"
	"time"
)

func TestGCRAIsATokenBucket(t *testing.T) {
	g := GCRA(10, 3) // 3 tokens, one per 100ms
	now := time.Unix(1_700_000_000, 0)
	var s State
	var d Decision
	for i := range 3 {
		if s, d = g.Step(s, now); !d.Allowed || d.Remaining != 2-i {
			t.Fatalf("call %d: %+v", i, d)
		}
	}

	s2, d := g.Step(s, now)
	if d.Allowed || d.RetryAfter != 100*time.Millisecond || s2 != s {
		t.Fatalf("fourth: %+v, state changed: %v", d, s2 != s)
	}

	if _, d := g.Step(s, now.Add(50*time.Millisecond)); d.Allowed || d.RetryAfter != 50*time.Millisecond {
		t.Fatalf("after 50ms: %+v", d)
	}

	if s, d = g.Step(s, now.Add(100*time.Millisecond)); !d.Allowed || d.Remaining != 0 {
		t.Fatalf("after 100ms: %+v", d)
	}

	// Refill caps at burst.
	for i := range 3 {
		if s, d = g.Step(s, now.Add(time.Hour)); !d.Allowed || d.Remaining != 2-i {
			t.Fatalf("after idle %d: %+v", i, d)
		}
	}

	if _, d = g.Step(s, now.Add(time.Hour)); d.Allowed {
		t.Fatalf("burst exceeded: %+v", d)
	}

	if g.TTL() != 600*time.Millisecond {
		t.Fatalf("TTL = %s, want 2 × burst/rate", g.TTL())
	}
}

func TestFixedWindowResetsAtBoundary(t *testing.T) {
	f := FixedWindow(2, time.Minute)
	base := time.Unix(1_700_000_040, 0) // divisible by 60
	var s State
	var d Decision
	s, d = f.Step(s, base.Add(50*time.Second))
	s, d = f.Step(s, base.Add(50*time.Second))
	if !d.Allowed || d.Remaining != 0 {
		t.Fatalf("second: %+v", d)
	}
	if s2, d := f.Step(s, base.Add(50*time.Second)); d.Allowed || d.RetryAfter != 10*time.Second || s2 != s {
		t.Fatalf("limited: %+v", d)
	}
	if _, d := f.Step(s, base.Add(60*time.Second)); !d.Allowed || d.Remaining != 1 {
		t.Fatalf("after boundary: %+v", d)
	}
	if _, d := f.Step(s, base.Add(5*time.Minute)); !d.Allowed || d.Remaining != 1 {
		t.Fatalf("much later: %+v", d)
	}
}

func TestSlidingWindowHasNoBoundaryBurst(t *testing.T) {
	w := SlidingWindow(10, time.Minute)
	base := time.Unix(1_700_000_040, 0)
	var s State
	at := base.Add(59 * time.Second)
	for range 10 {
		s, _ = w.Step(s, at)
	}
	// 2s past the boundary: previous=10, 1/30 elapsed, estimate 9.67.
	if _, d := w.Step(s, base.Add(62*time.Second)); d.Allowed {
		t.Fatalf("burst admitted across the boundary: %+v", d)
	}
	// Half way: estimate 5, room for 5.
	allowed := 0
	for range 10 {
		var d Decision
		if s, d = w.Step(s, base.Add(90*time.Second)); d.Allowed {
			allowed++
		}
	}
	if allowed != 5 {
		t.Fatalf("mid-window: allowed %d, want 5", allowed)
	}
}

func TestSlidingWindowForgetsAfterTwoWindows(t *testing.T) {
	w := SlidingWindow(2, time.Minute)
	base := time.Unix(1_700_000_040, 0)
	var s State
	s, _ = w.Step(s, base)
	s, _ = w.Step(s, base)
	for range 2 {
		var d Decision
		if s, d = w.Step(s, base.Add(3*time.Minute)); !d.Allowed {
			t.Fatalf("old windows still counted: %+v", d)
		}
	}
}

func TestAlgorithmValidation(t *testing.T) {
	bad := map[string]Algorithm{
		"GCRA rate 0":         GCRA(0, 1),
		"GCRA rate inf":       GCRA(math.Inf(1), 1),
		"GCRA rate too high":  GCRA(1e12, 1),
		"GCRA burst 0":        GCRA(1, 0),
		"GCRA span overflows": GCRA(1e-6, 100000), // 100000 × 11.6 days does not fit int64 ns
		"FixedWindow limit":   FixedWindow(0, time.Second),
		"FixedWindow window":  FixedWindow(1, 0),
		"SlidingWindow limit": SlidingWindow(0, time.Second),
		"SlidingWindow win":   SlidingWindow(1, -time.Second),
	}
	for name, a := range bad {
		if err := a.Validate(); !errors.Is(err, ErrInvalidOption) {
			t.Errorf("%s: Validate = %v", name, err)
		}
	}
	for _, a := range []Algorithm{GCRA(1, 1), FixedWindow(1, time.Second), SlidingWindow(1, time.Second)} {
		if err := a.Validate(); err != nil {
			t.Errorf("%s: %v", a.Name(), err)
		}
	}
}

// TestRetryAfterIsExactForEveryAlgorithm pins the contract Decision documents:
// a call one nanosecond before RetryAfter is refused, a call at RetryAfter is
// admitted.
func TestRetryAfterIsExactForEveryAlgorithm(t *testing.T) {
	base := time.Unix(1_700_000_040, 0)
	for name, alg := range map[string]Algorithm{
		"gcra":           GCRA(10, 3),
		"fixed_window":   FixedWindow(2, time.Minute),
		"sliding_window": SlidingWindow(4, time.Minute),
	} {
		t.Run(name, func(t *testing.T) {
			var s State
			var d Decision
			for d.Allowed = true; d.Allowed; {
				s, d = alg.Step(s, base)
			}
			if d.RetryAfter <= 0 {
				t.Fatalf("RetryAfter = %s on a refusal", d.RetryAfter)
			}
			if _, early := alg.Step(s, base.Add(d.RetryAfter-time.Nanosecond)); early.Allowed {
				t.Fatalf("admitted 1ns before RetryAfter %s", d.RetryAfter)
			}
			if _, then := alg.Step(s, base.Add(d.RetryAfter)); !then.Allowed {
				t.Fatalf("still refused at RetryAfter %s", d.RetryAfter)
			}
		})
	}
}

// TestSlidingWindowRetryAfterSurvivesRandomizedRoundingAcrossManyShapes drives
// random limits, windows and traffic shapes to a refusal and checks the same
// contract at each. The float64 formulation this replaced landed a rounding
// error above the limit at the boundary in about one refusal in six, refusing
// the retry with a RetryAfter of zero.
func TestSlidingWindowRetryAfterSurvivesRandomizedRoundingAcrossManyShapes(t *testing.T) {
	rng := rand.New(rand.NewPCG(1, 2))
	refused := 0
	for range 20000 {
		limit := 1 + rng.IntN(50)
		w := time.Duration(1+rng.IntN(120)) * time.Second
		alg := SlidingWindow(limit, w)
		base := time.Unix(1_700_000_040, 0).Truncate(w)
		var s State
		for range rng.IntN(limit + 1) { // some calls spread over the previous window
			s, _ = alg.Step(s, base.Add(time.Duration(rng.Int64N(int64(w)))))
		}
		now := base.Add(w + time.Duration(rng.Int64N(int64(w))))
		for range rng.IntN(limit + 3) { // and some at one instant of the current one
			s, _ = alg.Step(s, now)
		}
		_, d := alg.Step(s, now)
		if d.Allowed {
			continue
		}
		refused++
		if d.RetryAfter <= 0 {
			t.Fatalf("limit=%d window=%s state=%+v: RetryAfter = %s", limit, w, s, d.RetryAfter)
		}
		if _, early := alg.Step(s, now.Add(d.RetryAfter-time.Nanosecond)); early.Allowed {
			t.Fatalf("limit=%d window=%s state=%+v: admitted 1ns before RetryAfter %s", limit, w, s, d.RetryAfter)
		}
		if _, then := alg.Step(s, now.Add(d.RetryAfter)); !then.Allowed {
			t.Fatalf("limit=%d window=%s state=%+v: still refused at RetryAfter %s, next %s", limit, w, s, d.RetryAfter, then.RetryAfter)
		}
	}
	if refused < 1000 {
		t.Fatalf("only %d refusals exercised", refused)
	}
}
