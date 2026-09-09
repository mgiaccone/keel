package overload

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func must(t testing.TB, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

var _nameSeq atomic.Uint64

// uniqueName gives a limiter a name no other run in this process has used, so
// tests asserting absolute metric values are not confused by -count.
func uniqueName(t testing.TB) string {
	return fmt.Sprintf("%s#%d", t.Name(), _nameSeq.Add(1))
}

type fakeClock struct{ ns atomic.Int64 }

func newFakeClock() *fakeClock {
	c := &fakeClock{}
	c.ns.Store(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC).UnixNano())
	return c
}

func (c *fakeClock) Now() time.Time      { return time.Unix(0, c.ns.Load()) }
func (c *fakeClock) Add(d time.Duration) { c.ns.Add(int64(d)) }

// fixed is an Algorithm that never moves: the admission rules are what most of
// these tests are about, and a capacity that drifts under them would make
// every assertion about the shares a moving target.
type fixed struct {
	capacity  int
	shedBelow Priority
}

func (fixed) Name() string    { return "fixed" }
func (fixed) Validate() error { return nil }

func (f fixed) Step(s State, _ Signal, _ time.Time) (State, Decision) {
	return State{A: 1}, Decision{Capacity: f.capacity, ShedBelow: f.shedBelow}
}

// recorder is an Observer that remembers what it was told. The limiter calls
// observers under its own lock, so these methods only ever run serialised; the
// mutex is for the test goroutine reading them back.
type recorder struct {
	mu          sync.Mutex
	admitted    [3]int
	shed        [3]int
	waited      [3]int
	capacities  []int
	reentrant   bool
	reentrantOn *Limiter
}

func (r *recorder) Admitted(p Priority) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.admitted[p]++
}

func (r *recorder) Shed(p Priority) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.shed[p]++
}

func (r *recorder) Waited(p Priority) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.waited[p]++
}

func (r *recorder) CapacityChanged(c int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.capacities = append(r.capacities, c)
}

func (r *recorder) snapshot() ([3]int, [3]int, [3]int, []int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.admitted, r.shed, r.waited, append([]int(nil), r.capacities...)
}

// harness is a limiter on a fake clock with an attached recorder.
type harness struct {
	*Limiter
	clock  *fakeClock
	events *recorder
}

func newHarness(t testing.TB, a Algorithm, sheddableShare, defaultShare float64, opts ...Option) *harness {
	t.Helper()
	clock, events := newFakeClock(), &recorder{}
	l, err := New(uniqueName(t), a, sheddableShare, defaultShare,
		append([]Option{WithClock(clock.Now), WithObserver(events), WithSeed(1, 2)}, opts...)...)
	must(t, err)
	return &harness{Limiter: l, clock: clock, events: events}
}

// acquire is Acquire with the test's own context and a fatal on an unexpected
// error shape.
func (h *harness) acquire(t testing.TB, p Priority) func(error) {
	t.Helper()
	release, err := h.Acquire(t.Context(), p)
	if err != nil {
		t.Fatalf("Acquire(%s) = %v, want admitted", p, err)
	}
	return release
}

// shed asserts that a request at p is refused, and returns the refusal.
func (h *harness) shed(t testing.TB, p Priority) error {
	t.Helper()
	release, err := h.Acquire(t.Context(), p)
	if err == nil {
		release(nil)
		t.Fatalf("Acquire(%s) was admitted, want shed", p)
	}
	return err
}

// tick advances the clock and runs the queue's own pass over it, which is what
// a release or an arrival would otherwise do. It is how a test drives the
// give-up rule without generating traffic that would itself queue.
func (h *harness) tick(d time.Duration) {
	h.clock.Add(d)
	h.mu.Lock()
	h.pump(h.now())
	h.mu.Unlock()
}

// waitFor blocks until cond holds, failing the test rather than hanging if it
// never does.
func waitFor(t testing.TB, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal("condition never became true")
		}
		time.Sleep(time.Millisecond)
	}
}

func TestMinimalRoundTrip(t *testing.T) {
	h := newHarness(t, Gradient(1, 8), 0.6, 0.85)
	release := h.acquire(t, Default)
	if s := h.Stats(); s.Requests != 1 || s.Admitted != 1 || s.InFlight != 1 || s.Capacity != 8 {
		t.Fatalf("stats while in flight = %+v", s)
	}

	h.clock.Add(time.Millisecond)
	release(nil)
	if s := h.Stats(); s.InFlight != 0 || s.Shed != 0 {
		t.Fatalf("stats after release = %+v", s)
	}
}

func TestSharesRefuseTheLeastImportantBandFirst(t *testing.T) {
	h := newHarness(t, fixed{capacity: 10}, 0.6, 0.85) // 6 sheddable, 8 default, 10 critical
	var held []func(error)
	for range 6 {
		held = append(held, h.acquire(t, Sheddable))
	}
	if err := h.shed(t, Sheddable); !errors.Is(err, ErrOverloaded) {
		t.Fatalf("seventh sheddable: err = %v, want ErrOverloaded", err)
	}

	for range 2 {
		held = append(held, h.acquire(t, Default))
	}
	h.shed(t, Default)

	for range 2 {
		held = append(held, h.acquire(t, Critical))
	}
	// Critical is admitted up to the whole capacity and no further: there is
	// no band this package refuses to shed.
	if err := h.shed(t, Critical); !errors.Is(err, ErrOverloaded) {
		t.Fatalf("eleventh critical: err = %v, want ErrOverloaded", err)
	}

	s := h.Stats()
	if s.InFlight != 10 || s.Admitted != 10 || s.Shed != 3 || s.Requests != 13 {
		t.Fatalf("stats = %+v", s)
	}
	if s.ShedByPriority != [3]uint64{1, 1, 1} {
		t.Fatalf("shed by priority = %v, want one of each", s.ShedByPriority)
	}
	for _, release := range held {
		release(nil)
	}
}

func TestShedBelowRefusesABandTheSharesWouldStillAllow(t *testing.T) {
	h := newHarness(t, fixed{capacity: 10, shedBelow: Default}, 0.6, 0.85)
	if err := h.shed(t, Sheddable); !errors.Is(err, ErrOverloaded) {
		t.Fatalf("err = %v, want ErrOverloaded with an idle limiter shedding below Default", err)
	}
	h.acquire(t, Default)(nil)
}

func TestShedErrorBridgesToErrOverloadedAndStopsARetrier(t *testing.T) {
	err := &ShedError{Priority: Critical}
	if !errors.Is(err, ErrOverloaded) {
		t.Error("a ShedError must match ErrOverloaded")
	}
	if err.Retryable() {
		t.Error("Retryable must be false: retrying an overload inside the overloaded process is amplification")
	}

	var r interface{ Retryable() bool }
	if !errors.As(error(err), &r) {
		t.Fatal("a ShedError must satisfy the Retryable contract package retry looks for")
	}
}

func TestAcquireRejectsAnUnknownPriorityWithoutCountingIt(t *testing.T) {
	h := newHarness(t, fixed{capacity: 4}, 0.6, 0.85)
	release, err := h.Acquire(t.Context(), Priority(9))
	if release != nil || !errors.Is(err, ErrInvalidOption) {
		t.Fatalf("Acquire(9) returned a release func = %v, err = %v; want nil, ErrInvalidOption", release != nil, err)
	}
	if s := h.Stats(); s.Requests != 0 || s.Shed != 0 {
		t.Fatalf("stats = %+v, want an unusable priority counted nowhere", s)
	}
}

func TestReleasingTwiceDoesNotFreeTwoSlots(t *testing.T) {
	h := newHarness(t, fixed{capacity: 2}, 0.6, 0.85)
	release := h.acquire(t, Critical)
	release(nil)
	release(nil)
	if s := h.Stats(); s.InFlight != 0 {
		t.Fatalf("in flight = %d, want 0: a second release must be ignored, not counted", s.InFlight)
	}

	// A negative in-flight count would show up here as capacity that does not
	// exist, so take the whole thing and check it runs out where it should.
	first, second := h.acquire(t, Critical), h.acquire(t, Critical)
	h.shed(t, Critical)
	first(nil)
	second(nil)
}

func TestCapacityChangeReachesTheObserverOnceEach(t *testing.T) {
	h := newHarness(t, AIMD(1, 4, time.Millisecond, time.Millisecond), 0.6, 0.85)

	// Fast requests at max: capacity is already there, so nothing changes.
	for range 3 {
		release := h.acquire(t, Critical)
		h.clock.Add(time.Microsecond)
		release(nil)
	}
	if _, _, _, caps := h.events.snapshot(); len(caps) != 0 {
		t.Fatalf("capacities = %v, want none: capacity started at max and stayed there", caps)
	}

	// One slow request opens a bad run, a second past the interval halves it.
	for range 2 {
		release := h.acquire(t, Critical)
		h.clock.Add(time.Second)
		release(errors.New("boom"))
	}
	if _, _, _, caps := h.events.snapshot(); len(caps) != 1 || caps[0] != 2 {
		t.Fatalf("capacities = %v, want exactly one change to 2", caps)
	}
}

func TestQueueGrantsToTheHighestPriorityWaiterFirst(t *testing.T) {
	h := newHarness(t, fixed{capacity: 10}, 0.6, 0.85, WithMaxWait(time.Minute, time.Hour, time.Hour))
	var held []func(error)
	for range 10 {
		held = append(held, h.acquire(t, Critical))
	}

	sheddable := queue(t, h, Sheddable)
	waitFor(t, func() bool { return h.Stats().Waited == 1 })
	critical := queue(t, h, Critical)
	waitFor(t, func() bool { return h.Stats().Waited == 2 })

	// The sheddable waiter arrived first; the one freed slot goes to the
	// critical one anyway, because the queue orders by band before arrival.
	held[0](nil)
	release := mustGrant(t, critical)
	select {
	case got := <-sheddable:
		t.Fatalf("sheddable waiter resolved as %v while a critical one was ahead of it", got.err)
	case <-time.After(20 * time.Millisecond):
	}

	// Sheddable's share is six of the ten slots, so it waits until in-flight
	// has fallen that far, however long the critical work ahead of it takes.
	release(nil)
	for _, r := range held[1:5] {
		r(nil)
	}
	mustGrant(t, sheddable)(nil)
	for _, r := range held[5:] {
		r(nil)
	}
	if s := h.Stats(); s.WaitedThenAdmitted != 2 || s.WaitedThenShed != 0 {
		t.Fatalf("stats = %+v", s)
	}
}

func TestWaitingRequestIsShedWhenItsContextEnds(t *testing.T) {
	h := newHarness(t, fixed{capacity: 1}, 0.6, 0.85, WithMaxWait(time.Minute, time.Hour, time.Hour))
	held := h.acquire(t, Critical)
	defer held(nil)

	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() {
		_, err := h.Acquire(ctx, Default)
		done <- err
	}()
	waitFor(t, func() bool { return h.Stats().Waited == 1 })

	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled: the caller's own context wins over the queue", err)
	}
	// The request was not served, so the identity counts it as shed even
	// though the refusal was not this limiter's decision.
	if s := h.Stats(); s.Shed != 1 || s.WaitedThenShed != 1 || s.Admitted+s.Shed != s.Requests {
		t.Fatalf("stats = %+v", s)
	}
}

func TestQueueGivesUpOnWaitersOnceSojournStaysOverTarget(t *testing.T) {
	const target, interval = 10 * time.Millisecond, 100 * time.Millisecond
	h := newHarness(t, fixed{capacity: 1}, 0.6, 0.85, WithMaxWait(time.Hour, target, interval))
	held := h.acquire(t, Critical)
	defer held(nil)

	waiter := queue(t, h, Default)
	waitFor(t, func() bool { return h.Stats().Waited == 1 })

	// The ceiling is an hour away, so only the give-up rule can end this wait —
	// and not before a whole interval of waits above target has accumulated.
	for range _waitBuckets - 1 {
		h.tick(interval / _waitBuckets)
	}
	select {
	case got := <-waiter:
		t.Fatalf("gave up after less than the %s interval: %v", interval, got.err)
	case <-time.After(20 * time.Millisecond):
	}

	for range _waitBuckets + 2 {
		h.tick(interval / _waitBuckets)
	}
	select {
	case got := <-waiter:
		if !errors.Is(got.err, ErrOverloaded) {
			t.Fatalf("err = %v, want ErrOverloaded", got.err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the give-up rule never fired over two intervals of waits above target")
	}
	if s := h.Stats(); s.WaitedThenShed != 1 {
		t.Fatalf("stats = %+v", s)
	}
}

// TestTheGiveUpRuleMeasuresElapsedTimeNotSamplingRate is a real regression. The
// queue is only ever looked at when a request arrives or releases, so how many
// samples a trailing window holds says as much about traffic as about the
// queue: a rule reading bucket occupancy alone answers differently for a
// service sampled once a millisecond and one sampled twice an interval, and on
// the sparse one never fires at all — every waiter then rides out the full
// ceiling, which is the memory the rule exists to not spend.
func TestTheGiveUpRuleMeasuresElapsedTimeNotSamplingRate(t *testing.T) {
	const target, interval = 10 * time.Millisecond, 100 * time.Millisecond
	h := newHarness(t, fixed{capacity: 10}, 0.6, 0.85, WithMaxWait(time.Hour, target, interval))
	var held []func(error)
	for range 10 {
		held = append(held, h.acquire(t, Critical))
	}
	defer func() {
		for _, r := range held {
			r(nil)
		}
	}()

	waiter := queue(t, h, Sheddable)
	waitFor(t, func() bool { return h.Stats().Waited == 1 })

	// Half an interval per tick: five times fewer samples than the window has
	// buckets, so most buckets never receive one at all.
	for range 3 {
		h.tick(interval / 2)
	}
	select {
	case got := <-waiter:
		if !errors.Is(got.err, ErrOverloaded) {
			t.Fatalf("err = %v, want ErrOverloaded", got.err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the rule never fired on a queue sampled less often than once a bucket")
	}
}

// TestAQueueThatKeepsUpIsNeverGivenUpOn is the other half of the give-up rule.
// A queue can be continuously occupied for many intervals without being backed
// up at all, and waits that come in under target have to hold the rule off
// however long that goes on.
func TestAQueueThatKeepsUpIsNeverGivenUpOn(t *testing.T) {
	const target, interval = 50 * time.Millisecond, 100 * time.Millisecond
	h := newHarness(t, fixed{capacity: 4}, 0.6, 0.85, WithMaxWait(time.Hour, target, interval))
	var held []func(error)
	for range 4 {
		held = append(held, h.acquire(t, Critical))
	}

	for i := range 30 { // three intervals' worth of continuous queueing
		w := queue(t, h, Critical)
		waitFor(t, func() bool { return h.Stats().Waited == uint64(i+1) })

		h.tick(10 * time.Millisecond) // samples a 10ms wait, well inside target
		held[0](nil)                  // frees the slot, which the queue hands to w
		held[0] = mustGrant(t, w)
	}

	for _, r := range held {
		r(nil)
	}
	if s := h.Stats(); s.WaitedThenShed != 0 || s.WaitedThenAdmitted != 30 {
		t.Fatalf("stats = %+v, want nothing given up on a queue that is keeping up", s)
	}
}

// TestABrisklyServedBandDoesNotHideAStarvedOne pins which waiter the give-up
// rule reads: the queue's oldest, whatever band it is in, never whichever
// waiter happens to be granted a slot. Sampling grants instead would let
// critical traffic churning through in milliseconds supply an endless run of
// short waits while the sheddable waiter behind it starves — precisely the band
// the rule exists to drop first.
func TestABrisklyServedBandDoesNotHideAStarvedOne(t *testing.T) {
	const target, interval = 10 * time.Millisecond, 100 * time.Millisecond
	h := newHarness(t, fixed{capacity: 10}, 0.6, 0.85, WithMaxWait(time.Hour, target, interval))
	var held []func(error)
	for range 10 {
		held = append(held, h.acquire(t, Critical))
	}

	// Sheddable's share is six of the ten slots, so this waiter cannot be
	// admitted while critical work holds all ten.
	starved := queue(t, h, Sheddable)
	waitFor(t, func() bool { return h.Stats().Waited == 1 })

	for i := range 20 {
		churn := queue(t, h, Critical)
		waitFor(t, func() bool { return h.Stats().Waited == uint64(i+2) })
		held[0](nil)
		held[0] = mustGrant(t, churn) // served at once: a fresh, short wait

		h.tick(interval / _waitBuckets)
		select {
		case got := <-starved:
			if !errors.Is(got.err, ErrOverloaded) {
				t.Fatalf("err = %v, want ErrOverloaded", got.err)
			}
			for _, r := range held {
				r(nil)
			}
			return
		default:
		}
	}
	t.Fatal("the starved sheddable waiter was never given up on")
}

func TestQueueCeilingShedsAWaiterNoSlotEverReaches(t *testing.T) {
	h := newHarness(t, fixed{capacity: 1}, 0.6, 0.85, WithMaxWait(20*time.Millisecond, time.Hour, time.Hour))
	held := h.acquire(t, Critical)
	defer held(nil)

	start := time.Now()
	_, err := h.Acquire(t.Context(), Default)
	if !errors.Is(err, ErrOverloaded) {
		t.Fatalf("err = %v, want ErrOverloaded once the ceiling elapsed", err)
	}
	if waited := time.Since(start); waited < 20*time.Millisecond {
		t.Fatalf("gave up after %s, want at least the 20ms ceiling", waited)
	}
	if s := h.Stats(); s.Waited != 1 || s.WaitedThenShed != 1 || s.Admitted+s.Shed != s.Requests {
		t.Fatalf("stats = %+v", s)
	}
}

func TestAdmissionVetoesWithoutHoldingASlot(t *testing.T) {
	h := newHarness(t, fixed{capacity: 2}, 0.6, 0.85)
	veto := Admission(h.Limiter, Critical)

	// The veto has no release, so nothing it lets through is ever in flight;
	// running it a hundred times still leaves the whole capacity free.
	for range 100 {
		if err := veto(t.Context()); err != nil {
			t.Fatalf("veto = %v, want nil with an idle limiter", err)
		}
	}
	if s := h.Stats(); s.InFlight != 0 || s.Admitted != 100 {
		t.Fatalf("stats = %+v", s)
	}

	first, second := h.acquire(t, Critical), h.acquire(t, Critical)
	if err := veto(t.Context()); !errors.Is(err, ErrOverloaded) {
		t.Fatalf("veto = %v, want ErrOverloaded with capacity full", err)
	}
	first(nil)
	second(nil)
	if err := Admission(h.Limiter, Priority(7))(t.Context()); !errors.Is(err, ErrInvalidOption) {
		t.Fatalf("veto at an unknown priority = %v, want ErrInvalidOption", err)
	}
}

func TestNewRejectsEveryInvalidArgumentAtOnce(t *testing.T) {
	for name, tc := range map[string]struct {
		name           string
		algorithm      Algorithm
		sheddable, def float64
		opts           []Option
	}{
		"empty name":         {"", Gradient(1, 4), 0.6, 0.85, nil},
		"nil algorithm":      {"x", nil, 0.6, 0.85, nil},
		"invalid algorithm":  {"x", Gradient(0, 4), 0.6, 0.85, nil},
		"zero share":         {"x", Gradient(1, 4), 0, 0.85, nil},
		"shares inverted":    {"x", Gradient(1, 4), 0.9, 0.85, nil},
		"shares equal":       {"x", Gradient(1, 4), 0.85, 0.85, nil},
		"default above one":  {"x", Gradient(1, 4), 0.6, 1.5, nil},
		"nil clock":          {"x", Gradient(1, 4), 0.6, 0.85, []Option{WithClock(nil)}},
		"nil observer":       {"x", Gradient(1, 4), 0.6, 0.85, []Option{WithObserver(nil)}},
		"negative max wait":  {"x", Gradient(1, 4), 0.6, 0.85, []Option{WithMaxWait(-1, time.Second, time.Second)}},
		"wait without rule":  {"x", Gradient(1, 4), 0.6, 0.85, []Option{WithMaxWait(time.Second, 0, time.Second)}},
		"interval too short": {"x", Gradient(1, 4), 0.6, 0.85, []Option{WithMaxWait(time.Second, time.Second, 1)}},
	} {
		t.Run(name, func(t *testing.T) {
			l, err := New(tc.name, tc.algorithm, tc.sheddable, tc.def, tc.opts...)
			if l != nil || !errors.Is(err, ErrInvalidOption) {
				t.Fatalf("New = %v, %v; want nil, ErrInvalidOption", l, err)
			}
		})
	}
}

func TestNewReportsEveryProblemNotJustTheFirst(t *testing.T) {
	_, err := New("", nil, 2, 1, WithClock(nil))
	if err == nil {
		t.Fatal("want an error")
	}
	var joined interface{ Unwrap() []error }
	if !errors.As(err, &joined) || len(joined.Unwrap()) != 4 {
		t.Fatalf("err = %v, want four joined problems", err)
	}
}

func TestSwitchingTheQueueOffIsSpelledExplicitly(t *testing.T) {
	h := newHarness(t, fixed{capacity: 1}, 0.6, 0.85, WithMaxWait(0, 0, 0))
	held := h.acquire(t, Critical)
	defer held(nil)

	start := time.Now()
	h.shed(t, Critical)
	if waited := time.Since(start); waited > 50*time.Millisecond {
		t.Fatalf("refusal took %s: WithMaxWait(0, 0, 0) must not queue", waited)
	}
	if s := h.Stats(); s.Waited != 0 {
		t.Fatalf("stats = %+v, want nothing queued", s)
	}
}

func TestStatsStringIsOneLine(t *testing.T) {
	s := Stats{
		Name: "api", Algorithm: "gradient", Requests: 13, Admitted: 10, Shed: 3,
		ShedByPriority: [3]uint64{1, 1, 1}, Waited: 2, WaitedThenAdmitted: 1, WaitedThenShed: 1,
		Capacity: 10, InFlight: 4,
	}
	const want = "overload: name=api algorithm=gradient requests=13 admitted=10 shed=3 sheddable=1 default=1 critical=1 waited=2 waited_admitted=1 waited_shed=1 capacity=10 in_flight=4"
	if got := s.String(); got != want {
		t.Fatalf("String() =\n%s\nwant\n%s", got, want)
	}
}

// TestAdmissionMatchesAReferenceModel replays a scripted sequence of
// acquisitions and releases against a hand-written model of the rule the
// package documents — in flight below the band's share of capacity — and
// requires the limiter to agree on every single decision. A change to the
// admission rule has to change this model too, which is the point.
func TestAdmissionMatchesAReferenceModel(t *testing.T) {
	const capacity = 12
	h := newHarness(t, fixed{capacity: capacity}, 0.5, 0.75)

	bandLimit := func(p Priority) int {
		switch p {
		case Sheddable:
			return capacity / 2
		case Default:
			return capacity * 3 / 4
		default:
			return capacity
		}
	}

	var held []func(error)
	inFlight := 0
	rng := rand.New(rand.NewPCG(7, 11))
	for step := range 4000 {
		if len(held) > 0 && rng.IntN(2) == 0 {
			i := rng.IntN(len(held))
			held[i](nil)
			held = append(held[:i], held[i+1:]...)
			inFlight--
			continue
		}

		p := Priority(rng.IntN(3))
		want := inFlight < bandLimit(p)
		release, err := h.Acquire(t.Context(), p)
		if got := err == nil; got != want {
			t.Fatalf("step %d: Acquire(%s) admitted = %v with %d in flight, want %v", step, p, got, inFlight, want)
		}
		if want {
			held = append(held, release)
			inFlight++
		}
		if s := h.Stats(); s.InFlight != inFlight {
			t.Fatalf("step %d: in flight = %d, model says %d", step, s.InFlight, inFlight)
		}
	}

	for _, release := range held {
		release(nil)
	}
}

// TestStatsIdentitiesUnderChaos runs the whole limiter concurrently with
// random priorities, outcomes, timings and a queue in play, and checks the
// identities Stats documents afterwards. It is the test that has to hold
// however the internals are rearranged.
func TestStatsIdentitiesUnderChaos(t *testing.T) {
	rounds, workers := 400, 16
	if testing.Short() {
		rounds, workers = 40, 4
	}

	events := &recorder{}
	l, err := New(uniqueName(t), Gradient(2, 24), 0.5, 0.8,
		WithObserver(events), WithSeed(3, 5),
		WithMaxWait(3*time.Millisecond, 500*time.Microsecond, time.Millisecond))
	must(t, err)

	var wg sync.WaitGroup
	for w := range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			rng := rand.New(rand.NewPCG(uint64(w), 99))
			for range rounds {
				ctx, cancel := context.WithCancel(context.Background())
				if rng.IntN(8) == 0 {
					cancel() // some callers give up on their own
				}

				release, err := l.Acquire(ctx, Priority(rng.IntN(3)))
				if err == nil {
					if d := time.Duration(rng.IntN(300)) * time.Microsecond; d > 0 {
						time.Sleep(d)
					}
					var outcome error
					if rng.IntN(5) == 0 {
						outcome = errors.New("boom")
					}
					release(outcome)
					release(outcome) // a double release must never be visible
				}
				cancel()
			}
		}()
	}
	wg.Wait()

	s := l.Stats()
	if s.Admitted+s.Shed != s.Requests {
		t.Errorf("admitted %d + shed %d != requests %d", s.Admitted, s.Shed, s.Requests)
	}
	if total := s.ShedByPriority[0] + s.ShedByPriority[1] + s.ShedByPriority[2]; total != s.Shed {
		t.Errorf("shed by priority sums to %d, want %d", total, s.Shed)
	}
	if s.WaitedThenAdmitted+s.WaitedThenShed != s.Waited {
		t.Errorf("waited then admitted %d + shed %d != waited %d", s.WaitedThenAdmitted, s.WaitedThenShed, s.Waited)
	}
	if s.Waited > s.Requests {
		t.Errorf("waited %d exceeds requests %d", s.Waited, s.Requests)
	}
	if s.InFlight != 0 {
		t.Errorf("in flight = %d, want 0 once every release has run", s.InFlight)
	}
	if s.Capacity < 2 || s.Capacity > 24 {
		t.Errorf("capacity = %d, want it inside Gradient's [2, 24]", s.Capacity)
	}

	admitted, shed, waited, _ := events.snapshot()
	var seenAdmitted, seenShed, seenWaited uint64
	for p := range 3 {
		seenAdmitted += uint64(admitted[p])
		seenShed += uint64(shed[p])
		seenWaited += uint64(waited[p])
		if uint64(shed[p]) != s.ShedByPriority[p] {
			t.Errorf("observer saw %d shed at %s, stats says %d", shed[p], Priority(p), s.ShedByPriority[p])
		}
	}
	if seenAdmitted != s.Admitted || seenShed != s.Shed || seenWaited != s.Waited {
		t.Errorf("observer saw %d/%d/%d admitted/shed/waited, stats says %d/%d/%d",
			seenAdmitted, seenShed, seenWaited, s.Admitted, s.Shed, s.Waited)
	}
}

type grant struct {
	release func(error)
	err     error
}

// queue starts an Acquire that is expected to block, and reports how it ended.
func queue(t testing.TB, h *harness, p Priority) <-chan grant {
	t.Helper()
	ch := make(chan grant, 1)
	go func() {
		release, err := h.Acquire(context.Background(), p)
		ch <- grant{release: release, err: err}
	}()
	return ch
}

func mustGrant(t testing.TB, ch <-chan grant) func(error) {
	t.Helper()
	select {
	case got := <-ch:
		if got.err != nil {
			t.Fatalf("queued Acquire = %v, want a slot", got.err)
		}
		return got.release
	case <-time.After(5 * time.Second):
		t.Fatal("queued Acquire never resolved")
		return nil
	}
}

// The benchmarks pin capacity so they measure the admission gate rather than
// whichever way an estimator happened to drift; the algorithms are timed on
// their own below.
func BenchmarkAcquire(b *testing.B) {
	l, err := New("bench", fixed{capacity: 1 << 20}, 0.6, 0.85)
	must(b, err)
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		release, err := l.Acquire(context.Background(), Default)
		if err != nil {
			b.Fatal(err)
		}
		release(nil)
	}
}

func BenchmarkAcquireParallel(b *testing.B) {
	l, err := New("bench-parallel", fixed{capacity: 1 << 20}, 0.6, 0.85)
	must(b, err)
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			release, err := l.Acquire(context.Background(), Default)
			if err != nil {
				b.Fatal(err)
			}
			release(nil)
		}
	})
}

func BenchmarkAlgorithmStep(b *testing.B) {
	for name, a := range map[string]Algorithm{
		"gradient": Gradient(1, 4096),
		"aimd":     AIMD(1, 4096, time.Millisecond, time.Second),
	} {
		b.Run(name, func(b *testing.B) {
			now := time.Now()
			s, _ := a.Step(State{}, Signal{}, now)
			sig := Signal{Duration: 900 * time.Microsecond}
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				s, _ = a.Step(s, sig, now)
			}
		})
	}
}

func BenchmarkAcquireShed(b *testing.B) {
	l, err := New("bench-shed", fixed{capacity: 1}, 0.6, 0.85)
	must(b, err)
	release, err := l.Acquire(context.Background(), Critical)
	must(b, err)
	defer release(nil)

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if _, err := l.Acquire(context.Background(), Default); err == nil {
			b.Fatal("want a refusal")
		}
	}
}
