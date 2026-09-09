package overload

import (
	"errors"
	"math/rand/v2"
	"testing"
	"time"
)

var _errBoom = errors.New("boom")

// drive replays signals through an algorithm and returns the capacity it ended
// on, so a test can state a trajectory rather than a single step.
func drive(a Algorithm, s State, now time.Time, n int, sig func(i int) Signal) (State, time.Time, int) {
	d := Decision{}
	for i := range n {
		now = now.Add(time.Millisecond)
		s, d = a.Step(s, sig(i), now)
	}
	return s, now, d.Capacity
}

func TestAlgorithmValidation(t *testing.T) {
	for name, a := range map[string]Algorithm{
		"gradient min zero":      Gradient(0, 10),
		"gradient min negative":  Gradient(-1, 10),
		"gradient max below min": Gradient(10, 9),
		"aimd min zero":          AIMD(0, 10, time.Second, time.Second),
		"aimd max below min":     AIMD(10, 9, time.Second, time.Second),
		"aimd target zero":       AIMD(1, 10, 0, time.Second),
		"aimd interval zero":     AIMD(1, 10, time.Second, 0),
	} {
		t.Run(name, func(t *testing.T) {
			if err := a.Validate(); !errors.Is(err, ErrInvalidOption) {
				t.Fatalf("Validate = %v, want ErrInvalidOption", err)
			}
		})
	}
}

func TestZeroStateAnswersWithTheOpeningCapacity(t *testing.T) {
	for name, a := range map[string]Algorithm{
		"gradient": Gradient(3, 40),
		"aimd":     AIMD(3, 40, time.Second, time.Second),
	} {
		t.Run(name, func(t *testing.T) {
			if a.Name() != name {
				t.Errorf("Name = %q, want %q", a.Name(), name)
			}
			// The zero State is the contract New relies on to learn what
			// capacity to open with, before any request has run.
			s, d := a.Step(State{}, Signal{}, time.Now())
			if d.Capacity != 40 {
				t.Errorf("opening capacity = %d, want max", d.Capacity)
			}
			if d.ShedBelow != Sheddable {
				t.Errorf("opening ShedBelow = %s, want nothing shed on that account", d.ShedBelow)
			}
			if s == (State{}) {
				t.Error("the state returned must not be the zero State, or every step would re-initialise")
			}
		})
	}
}

func TestGradientShrinksOnDriftFromItsBaselineAndRecoversOnANewBest(t *testing.T) {
	a := Gradient(2, 100)
	s, now := State{A: 100 * _scale}, time.Now()

	// A first sample only sets the baseline: there is nothing to compare it
	// against, so capacity must not move on it.
	s, now, capacity := drive(a, s, now, 1, func(int) Signal { return Signal{Duration: time.Millisecond} })
	if capacity != 100 {
		t.Fatalf("capacity = %d after one sample, want max: one sample is a baseline, not a gradient", capacity)
	}

	// Sustained service time a hundred times the baseline is the queue
	// building, and capacity has to come down for it.
	s, now, capacity = drive(a, s, now, 60, func(int) Signal { return Signal{Duration: 100 * time.Millisecond} })
	if capacity > 10 || capacity < 2 {
		t.Fatalf("capacity = %d after 60 slow requests, want well below max and inside [2, 100]", capacity)
	}

	// A genuinely fast request replaces the baseline at once, and the headroom
	// probe is what lets capacity climb back at all.
	_, _, capacity = drive(a, s, now, 300, func(int) Signal { return Signal{Duration: time.Millisecond} })
	if capacity != 100 {
		t.Fatalf("capacity = %d after 300 fast requests, want back at max", capacity)
	}
}

// TestGradientRecalibratesToAServiceThatStaysSlow pins the failure mode the
// doc comment warns about, rather than leaving it as prose: a service that is
// slower for good gets its new speed adopted as the baseline, and capacity
// comes back up around it. That is wanted — the alternative is a limiter stuck
// at its floor forever on the strength of one lucky request — but it does mean
// the baseline is only ever as good as the best request it has seen.
func TestGradientRecalibratesToAServiceThatStaysSlow(t *testing.T) {
	a := Gradient(2, 100)
	s, now := State{A: 100 * _scale}, time.Now()
	s, now, _ = drive(a, s, now, 1, func(int) Signal { return Signal{Duration: time.Millisecond} })

	s, now, bottom := drive(a, s, now, 60, func(int) Signal { return Signal{Duration: 100 * time.Millisecond} })
	_, _, later := drive(a, s, now, 140, func(int) Signal { return Signal{Duration: 100 * time.Millisecond} })
	if later <= bottom {
		t.Fatalf("capacity went %d then %d under an unchanging 100ms; want the baseline to have followed it up", bottom, later)
	}
}

func TestGradientTreatsAFailureAsTheStrongestEvidenceOfOverload(t *testing.T) {
	a := Gradient(4, 64)
	s, _ := a.Step(State{}, Signal{}, time.Now())

	// Fast, but failing: the latency says there is room and the outcome says
	// there is not, and the outcome wins.
	_, _, capacity := drive(a, s, time.Now(), 4, func(int) Signal {
		return Signal{Duration: time.Microsecond, Err: _errBoom}
	})
	if capacity != 4 {
		t.Fatalf("capacity = %d after four fast failures, want halved down to min", capacity)
	}
}

func TestGradientRefusesSheddableOnceItIsPinnedAtItsFloor(t *testing.T) {
	a := Gradient(4, 64)
	s, _ := a.Step(State{}, Signal{}, time.Now())
	_, _, _ = drive(a, s, time.Now(), 4, func(int) Signal { return Signal{Duration: time.Microsecond, Err: _errBoom} })

	_, d := a.Step(State{A: 4 * _scale, B: int64(time.Millisecond), C: 5}, Signal{Duration: time.Millisecond}, time.Now())
	if d.ShedBelow != Default {
		t.Fatalf("ShedBelow = %s at the floor, want Default", d.ShedBelow)
	}

	// A floor that is also the ceiling is a fixed limit, not a limiter that
	// has backed down, so nothing extra is refused for it.
	_, d = Gradient(4, 4).Step(State{A: 4 * _scale, C: 5}, Signal{Duration: time.Millisecond}, time.Now())
	if d.ShedBelow != Sheddable {
		t.Fatalf("ShedBelow = %s with min == max, want Sheddable", d.ShedBelow)
	}
}

// TestAIMDHalvesOnlyOnceRequestsHaveBeenBadForAWholeInterval is the property
// that makes AIMD usable on inbound traffic at all: without the hysteresis a
// single slow request on a quiet path collapses the limit.
func TestAIMDHalvesOnlyOnceRequestsHaveBeenBadForAWholeInterval(t *testing.T) {
	const target, interval = 100 * time.Millisecond, time.Second
	a := AIMD(2, 32, target, interval)
	now := time.Now()
	s, d := a.Step(State{}, Signal{}, now)
	if d.Capacity != 32 {
		t.Fatalf("opening capacity = %d, want max", d.Capacity)
	}

	// One slow request opens the run; nothing moves yet.
	s, d = a.Step(s, Signal{Duration: target + time.Millisecond}, now)
	if d.Capacity != 32 {
		t.Fatalf("capacity = %d after one slow request, want it untouched", d.Capacity)
	}

	// Still inside the interval, still nothing.
	s, d = a.Step(s, Signal{Duration: target * 10}, now.Add(interval-time.Millisecond))
	if d.Capacity != 32 {
		t.Fatalf("capacity = %d before the interval elapsed, want it untouched", d.Capacity)
	}

	// Past it, one halving, and only one: the next needs another full
	// interval, not another slow request.
	s, d = a.Step(s, Signal{Duration: target * 10}, now.Add(interval))
	if d.Capacity != 16 {
		t.Fatalf("capacity = %d once the interval elapsed, want 16", d.Capacity)
	}
	s, d = a.Step(s, Signal{Duration: target * 10}, now.Add(interval+time.Millisecond))
	if d.Capacity != 16 {
		t.Fatalf("capacity = %d, want no second halving inside the same interval", d.Capacity)
	}

	// One good request ends the run and starts the additive increase again.
	s, d = a.Step(s, Signal{Duration: time.Millisecond}, now.Add(2*interval))
	if d.Capacity != 17 {
		t.Fatalf("capacity = %d after a good request, want 17", d.Capacity)
	}
	_, d = a.Step(s, Signal{Duration: target * 10}, now.Add(3*interval))
	if d.Capacity != 17 {
		t.Fatalf("capacity = %d, want the halving to need a fresh full interval after the run was cleared", d.Capacity)
	}
}

// TestAMeasurementOfZeroIsNotAMissedTarget is a real regression: a clock too
// coarse to separate admission from release — the fake clock this package's own
// harness hands a request released without an intervening Add, and any
// low-resolution clock under a fast handler — reports Duration 0. Read as a
// request that missed its target rather than as no measurement at all, that
// walks capacity down to min and starts refusing Sheddable outright, purely
// from a run of instantaneous successes.
func TestAMeasurementOfZeroIsNotAMissedTarget(t *testing.T) {
	for name, a := range map[string]Algorithm{
		"gradient": Gradient(1, 10),
		"aimd":     AIMD(1, 10, 50*time.Millisecond, time.Second),
	} {
		t.Run(name, func(t *testing.T) {
			now := time.Now()
			s, _ := a.Step(State{}, Signal{}, now)

			d := Decision{}
			for i := range 5 {
				now = now.Add(time.Second) // far enough apart to clear any hysteresis
				s, d = a.Step(s, Signal{Duration: 0}, now)
				if d.Capacity != 10 || d.ShedBelow != Sheddable {
					t.Fatalf("step %d: %+v after successes with nothing measured, want capacity 10 and nothing extra shed", i, d)
				}
			}
		})
	}
}

func TestAIMDCountsAFailureAsBadHoweverFastItWas(t *testing.T) {
	const target, interval = time.Second, time.Second
	a := AIMD(1, 8, target, interval)
	now := time.Now()
	s, _ := a.Step(State{}, Signal{}, now)

	s, _ = a.Step(s, Signal{Duration: time.Nanosecond, Err: _errBoom}, now)
	_, d := a.Step(s, Signal{Duration: time.Nanosecond, Err: _errBoom}, now.Add(interval))
	if d.Capacity != 4 {
		t.Fatalf("capacity = %d, want a fast failure to count toward the decrease", d.Capacity)
	}
}

// TestEveryAlgorithmStaysInsideItsBoundsUnderChaos drives both implementations
// with a signal stream that mixes everything they can be handed — fast, slow,
// failing, zero-duration, queued and not, with the clock jumping about — and
// requires only what both of them promise: a capacity inside [min, max], and a
// ShedBelow that names a real priority.
func TestEveryAlgorithmStaysInsideItsBoundsUnderChaos(t *testing.T) {
	steps := 20_000
	if testing.Short() {
		steps = 2_000
	}

	for name, a := range map[string]Algorithm{
		"gradient": Gradient(3, 90),
		"aimd":     AIMD(3, 90, 20*time.Millisecond, 250*time.Millisecond),
	} {
		t.Run(name, func(t *testing.T) {
			must(t, a.Validate())
			rng := rand.New(rand.NewPCG(17, 23))
			now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
			s, d := a.Step(State{}, Signal{}, now)

			for i := range steps {
				now = now.Add(time.Duration(rng.IntN(int(time.Second))))
				sig := Signal{
					Waited:   rng.IntN(2) == 0,
					Sojourn:  time.Duration(rng.IntN(int(time.Second))),
					Duration: time.Duration(rng.IntN(int(time.Second))),
				}
				if rng.IntN(20) == 0 {
					sig.Duration = 0 // nothing measured
				}
				if rng.IntN(6) == 0 {
					sig.Err = _errBoom
				}
				s, d = a.Step(s, sig, now)

				if d.Capacity < 3 || d.Capacity > 90 {
					t.Fatalf("step %d: capacity = %d, want it inside [3, 90]", i, d.Capacity)
				}
				if d.ShedBelow > Critical {
					t.Fatalf("step %d: ShedBelow = %d, not a priority", i, d.ShedBelow)
				}
			}
		})
	}
}

// TestSojournNeverReachesTheCapacityEstimate is the separation the package
// promises: capacity and queueing are independent, so the same signals with
// and without a queue wait must produce the same trajectory.
func TestSojournNeverReachesTheCapacityEstimate(t *testing.T) {
	for name, a := range map[string]Algorithm{
		"gradient": Gradient(2, 50),
		"aimd":     AIMD(2, 50, 10*time.Millisecond, 100*time.Millisecond),
	} {
		t.Run(name, func(t *testing.T) {
			now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
			plain, _ := a.Step(State{}, Signal{}, now)
			queued := plain

			for i := range 500 {
				now = now.Add(time.Millisecond)
				d := time.Duration(1+i%40) * time.Millisecond
				plain, _ = a.Step(plain, Signal{Duration: d}, now)
				queued, _ = a.Step(queued, Signal{Duration: d, Waited: true, Sojourn: time.Minute}, now)
				if plain != queued {
					t.Fatalf("step %d: state diverged, %+v vs %+v, so the sojourn reached the estimate", i, plain, queued)
				}
			}
		})
	}
}
