package breaker

import (
	"context"
	"errors"
	"fmt"
	"math"
	"math/rand/v2"
	"runtime"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// fakeClock is the only synchronisation primitive in the test suite. It lives
// here, not in the breaker: the breaker only ever calls Now().
type fakeClock struct{ ns atomic.Int64 }

func newFakeClock() *fakeClock {
	c := &fakeClock{}
	c.ns.Store(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC).UnixNano())
	return c
}

func (c *fakeClock) Now() time.Time      { return time.Unix(0, c.ns.Load()) }
func (c *fakeClock) Add(d time.Duration) { c.ns.Add(int64(d)) }

var errBoom = errors.New("boom")

func TestMinimalRoundTrip(t *testing.T) {
	b, err := New(t.Name())
	if err != nil {
		t.Fatal(err)
	}
	defer b.Stop()
	v, err := b.Do(t.Context(), func(context.Context) (int, error) { return 42, nil })
	if err != nil || v != 42 {
		t.Fatalf("Do = %d, %v; want 42, nil", v, err)
	}
	if got := b.Stats(); got.Calls != 1 || got.Admitted != 1 || got.Successes != 1 {
		t.Fatalf("stats = %+v", got)
	}
}

// harness bundles a breaker with a fake clock, a fixed seed and jitter off so
// intervals are exact. Tests that need jitter pass their own OpenJitter.
type harness struct {
	*Breaker
	clock *fakeClock
}

func newHarness(t *testing.T, opts ...Option) *harness {
	t.Helper()
	return newNamedHarness(t, t.Name(), opts...)
}

// newNamedHarness is newHarness with an explicit breaker name, for tests that
// need several breakers or assert on the dependency label.
func newNamedHarness(t *testing.T, name string, opts ...Option) *harness {
	t.Helper()
	clock := newFakeClock()
	base := []Option{
		WithClock(clock.Now),
		WithSeed(1, 2),
		WithOpenJitter(0),
		WithOpenInterval(time.Second, 8*time.Second),
	}
	b, err := New(name, append(base, opts...)...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(b.Stop)
	return &harness{Breaker: b, clock: clock}
}

func (h *harness) fail(t *testing.T) error {
	t.Helper()
	_, err := h.Do(t.Context(), func(context.Context) (struct{}, error) { return struct{}{}, errBoom })
	return err
}

func (h *harness) ok(t *testing.T) error {
	t.Helper()
	_, err := h.Do(t.Context(), func(context.Context) (struct{}, error) { return struct{}{}, nil })
	return err
}

// trip drives a closed breaker open with FailureThreshold failures.
func (h *harness) trip(t *testing.T) {
	t.Helper()
	for range h.cfg.failureThreshold {
		if err := h.fail(t); !errors.Is(err, errBoom) {
			t.Fatalf("fail: got %v, want errBoom", err)
		}
	}
	h.wantState(t, Open)
}

func (h *harness) wantState(t *testing.T, want State) {
	t.Helper()
	if got := h.State(); got != want {
		t.Fatalf("state = %s, want %s\n%s", got, want, h.Stats())
	}
}

func TestTripsAfterConsecutiveFailures(t *testing.T) {
	h := newHarness(t, WithFailureThreshold(3))
	h.fail(t)
	h.fail(t)
	h.wantState(t, Closed)
	h.fail(t)
	h.wantState(t, Open)
	if s := h.Stats(); s.Trips != 1 || s.ConsecutiveTrips != 1 || s.Failures != 3 {
		t.Fatalf("stats = %+v", s)
	}
}

func TestSuccessResetsFailureRun(t *testing.T) {
	h := newHarness(t, WithFailureThreshold(3))
	h.fail(t)
	h.fail(t)
	h.ok(t)
	h.fail(t)
	h.fail(t)
	h.wantState(t, Closed)
	h.fail(t)
	h.wantState(t, Open)
}

func TestOpenRejectsWithoutInvokingFn(t *testing.T) {
	h := newHarness(t, WithFailureThreshold(2))
	h.trip(t)
	for range 10 {
		_, err := h.Do(t.Context(), func(context.Context) (int, error) {
			t.Fatal("fn invoked while open")
			return 0, nil
		})
		if !errors.Is(err, ErrOpen) {
			t.Fatalf("err = %v, want ErrOpen", err)
		}
	}
	s := h.Stats()
	if s.Rejected != 10 || s.Admitted != 2 || s.Calls != 12 {
		t.Fatalf("stats = %+v", s)
	}
	if s.NextProbeIn != time.Second {
		t.Fatalf("NextProbeIn = %s, want 1s", s.NextProbeIn)
	}
}

func TestOpenBecomesHalfOpenWhenIntervalElapses(t *testing.T) {
	h := newHarness(t, WithFailureThreshold(1))
	h.trip(t)
	h.clock.Add(999 * time.Millisecond)
	h.wantState(t, Open)
	h.clock.Add(time.Millisecond)
	h.wantState(t, HalfOpen)
	if s := h.Stats(); s.NextProbeIn != 0 {
		t.Fatalf("NextProbeIn = %s while half-open, want 0", s.NextProbeIn)
	}
}

func TestExpiryIsEvaluatedOnAdmitToo(t *testing.T) {
	h := newHarness(t, WithFailureThreshold(1))
	h.trip(t)
	h.clock.Add(time.Second)
	// No Stats call in between: the admit handler must do the transition.
	if err := h.ok(t); err != nil {
		t.Fatalf("probe rejected: %v", err)
	}
	h.wantState(t, HalfOpen)
}

// TestOneLuckyProbeDoesNotClose pins the default SuccessThreshold of 2. With
// 1, a single lucky probe against a still-sick backend would restore full
// traffic, which fails, which reopens: the flap the default exists to prevent.
func TestOneLuckyProbeDoesNotClose(t *testing.T) {
	h := newHarness(t, WithFailureThreshold(1))
	if h.cfg.successThreshold != 2 {
		t.Fatalf("default SuccessThreshold = %d, want 2", h.cfg.successThreshold)
	}
	h.trip(t)
	h.clock.Add(time.Second)
	h.wantState(t, HalfOpen)
	h.ok(t)
	h.wantState(t, HalfOpen)
	h.ok(t)
	h.wantState(t, Closed)
}

func TestFailedProbeReopens(t *testing.T) {
	h := newHarness(t, WithFailureThreshold(1), WithSuccessThreshold(3))
	h.trip(t)
	h.clock.Add(time.Second)
	h.ok(t)
	h.ok(t)
	h.wantState(t, HalfOpen)
	h.fail(t)
	h.wantState(t, Open)
	if s := h.Stats(); s.Trips != 2 || s.ConsecutiveTrips != 2 {
		t.Fatalf("stats = %+v", s)
	}
	// Successes before the failed probe do not carry over.
	h.clock.Add(2 * time.Second)
	h.ok(t)
	h.wantState(t, HalfOpen)
}

func TestMaxProbesEnforced(t *testing.T) {
	h := newHarness(t, WithFailureThreshold(1), WithMaxProbes(1))
	h.trip(t)
	h.clock.Add(time.Second)

	entered := make(chan struct{})
	finish := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		_, err := h.Do(context.Background(), func(context.Context) (int, error) {
			close(entered)
			<-finish
			return 0, nil
		})
		done <- err
	}()
	recv(t, entered)

	if err := h.ok(t); !errors.Is(err, ErrProbeLimit) {
		t.Fatalf("second probe: err = %v, want ErrProbeLimit", err)
	}
	if s := h.Stats(); s.Rejected != 1 {
		t.Fatalf("stats = %+v", s)
	}
	close(finish)
	if err := recv(t, done); err != nil {
		t.Fatalf("first probe: %v", err)
	}
	// The slot is free again.
	if err := h.ok(t); err != nil {
		t.Fatalf("after release: %v", err)
	}
	h.wantState(t, Closed)
}

func TestOpenIntervalGrowsExponentiallyAndCaps(t *testing.T) {
	h := newHarness(t, WithFailureThreshold(1), WithOpenInterval(time.Second, 8*time.Second))
	var got []time.Duration
	for range 6 {
		h.fail(t) // trips (first from closed, then from each half-open probe)
		s := h.Stats()
		got = append(got, s.NextProbeIn)
		h.clock.Add(s.NextProbeIn)
		h.wantState(t, HalfOpen)
	}
	want := []time.Duration{1 * time.Second, 2 * time.Second, 4 * time.Second, 8 * time.Second, 8 * time.Second, 8 * time.Second}
	if !slices.Equal(got, want) {
		t.Fatalf("intervals = %v, want %v", got, want)
	}
}

func TestOpenIntervalCapsOnNonPowerOfTwo(t *testing.T) {
	h := newHarness(t, WithFailureThreshold(1), WithOpenInterval(3*time.Second, 8*time.Second))
	var got []time.Duration
	for range 3 {
		h.fail(t)
		s := h.Stats()
		got = append(got, s.NextProbeIn)
		h.clock.Add(s.NextProbeIn)
	}
	want := []time.Duration{3 * time.Second, 6 * time.Second, 8 * time.Second}
	if !slices.Equal(got, want) {
		t.Fatalf("intervals = %v, want %v", got, want)
	}
}

// TestJitterSpreadsProbesAcrossInstances models six instances that trip on
// the same outage at the same instant; they must not probe together.
func TestJitterSpreadsProbesAcrossInstances(t *testing.T) {
	const instances = 6
	seen := make(map[time.Duration]bool)
	for i := range uint64(instances) {
		h := newHarness(t,
			WithFailureThreshold(1),
			WithOpenInterval(10*time.Second, time.Minute),
			WithOpenJitter(0.2),
			WithSeed(i+1, 99),
		)
		h.trip(t)
		d := h.Stats().NextProbeIn
		if d < 8*time.Second || d > 12*time.Second {
			t.Fatalf("instance %d: NextProbeIn = %s, outside ±20%% of 10s", i, d)
		}
		seen[d] = true
	}
	if len(seen) < instances-1 {
		t.Fatalf("only %d distinct probe times across %d instances: %v", len(seen), instances, seen)
	}
}

func TestJitteredDeadlineIsFixedForTheOpenPeriod(t *testing.T) {
	h := newHarness(t, WithFailureThreshold(1), WithOpenInterval(10*time.Second, time.Minute), WithOpenJitter(0.5))
	h.trip(t)
	first := h.Stats().NextProbeIn
	for range 20 {
		h.fail(t) // rejected; must not redraw the deadline
		if got := h.Stats().NextProbeIn; got != first {
			t.Fatalf("NextProbeIn changed from %s to %s without the clock moving", first, got)
		}
	}
}

func TestConsecutiveTripsResetsOnClose(t *testing.T) {
	h := newHarness(t, WithFailureThreshold(1), WithSuccessThreshold(1))
	h.fail(t)
	h.clock.Add(time.Second)
	h.fail(t)
	h.clock.Add(2 * time.Second)
	h.fail(t)
	if s := h.Stats(); s.ConsecutiveTrips != 3 || s.Trips != 3 {
		t.Fatalf("stats = %+v", s)
	}
	h.clock.Add(4 * time.Second)
	h.ok(t)
	h.wantState(t, Closed)
	if s := h.Stats(); s.ConsecutiveTrips != 0 || s.Trips != 3 {
		t.Fatalf("after close: stats = %+v", s)
	}
	// Next trip starts the backoff from OpenBase again.
	h.fail(t)
	if got := h.Stats().NextProbeIn; got != time.Second {
		t.Fatalf("NextProbeIn after reset = %s, want 1s", got)
	}
}

func TestIsFailureOverrideNotFoundIsSuccess(t *testing.T) {
	errNotFound := errors.New("not found")
	h := newHarness(t,
		WithFailureThreshold(2),
		WithIsFailure(func(err error) bool {
			return err != nil && !errors.Is(err, errNotFound)
		}),
	)
	for range 1000 {
		_, err := h.Do(t.Context(), func(context.Context) (int, error) { return 0, errNotFound })
		if !errors.Is(err, errNotFound) {
			t.Fatalf("Do rewrote the error: %v", err)
		}
	}
	h.wantState(t, Closed)
	if s := h.Stats(); s.Successes != 1000 || s.Failures != 0 {
		t.Fatalf("stats = %+v", s)
	}
}

func TestContextCanceledFromFnIsNeutral(t *testing.T) {
	h := newHarness(t, WithFailureThreshold(2))
	h.fail(t)
	// A cancellation neither trips nor resets the failure run.
	for range 5 {
		_, err := h.Do(t.Context(), func(context.Context) (int, error) {
			return 0, fmt.Errorf("query: %w", context.Canceled)
		})
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("err = %v", err)
		}
	}
	h.wantState(t, Closed)
	h.fail(t) // second consecutive failure: the cancellations did not reset the run
	h.wantState(t, Open)
	s := h.Stats()
	if s.Canceled != 5 || s.Failures != 2 || s.Successes != 0 {
		t.Fatalf("stats = %+v", s)
	}
	if s.Successes+s.Failures+s.Canceled != s.Admitted {
		t.Fatalf("accounting invariant broken: %+v", s)
	}
}

func TestDeadlineExceededIsAFailureByDefault(t *testing.T) {
	h := newHarness(t, WithFailureThreshold(1))
	h.Do(t.Context(), func(context.Context) (int, error) { return 0, context.DeadlineExceeded })
	h.wantState(t, Open)
}

// TestStaleOutcomeIsIgnored: a slow call admitted while closed must not close
// a circuit that tripped underneath it, however successful it turns out.
func TestStaleOutcomeIsIgnored(t *testing.T) {
	h := newHarness(t, WithFailureThreshold(2))

	entered := make(chan struct{})
	finish := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		_, err := h.Do(context.Background(), func(context.Context) (int, error) {
			close(entered)
			<-finish
			return 1, nil
		})
		done <- err
	}()
	recv(t, entered)

	h.trip(t)
	h.clock.Add(time.Second)
	h.wantState(t, HalfOpen)
	h.ok(t) // one genuine probe success; the stale one must not be the second

	close(finish)
	if err := recv(t, done); err != nil {
		t.Fatal(err)
	}
	h.wantState(t, HalfOpen)
	s := h.Stats()
	if s.Successes != 2 || s.Failures != 2 {
		t.Fatalf("stale outcome must still be counted: %+v", s)
	}
	// A stale settle must not free a probe slot either: the one genuine
	// probe slot is still available, exactly once.
	h.ok(t)
	h.wantState(t, Closed)
}

func TestOnStateChangeSequence(t *testing.T) {
	var got []string
	h := newHarness(t,
		WithFailureThreshold(1),
		WithOnStateChange(func(from, to State) {
			got = append(got, from.String()+"->"+to.String())
		}),
	)
	h.fail(t)
	h.clock.Add(time.Second)
	h.ok(t)
	h.ok(t)
	h.wantState(t, Closed)
	// Stats/State are answered by the loop after any hook has run, so the
	// read of got below is ordered after the appends by the channel replies.
	want := []string{"closed->open", "open->half-open", "half-open->closed"}
	if !slices.Equal(got, want) {
		t.Fatalf("transitions = %v, want %v", got, want)
	}
}

func TestOnStateChangeIsSynchronousWithDo(t *testing.T) {
	fired := make(chan struct{}, 1)
	h := newHarness(t,
		WithFailureThreshold(1),
		WithOnStateChange(func(from, to State) { fired <- struct{}{} }),
	)
	h.fail(t)
	select {
	case <-fired:
	default:
		t.Fatal("OnStateChange had not fired when Do returned")
	}
}

func TestConcurrentAccounting(t *testing.T) {
	const goroutines, perGoroutine = 200, 25
	h := newHarness(t, WithFailureThreshold(3), WithSuccessThreshold(1), WithOpenInterval(time.Millisecond, time.Minute))
	var wg sync.WaitGroup
	for g := range goroutines {
		wg.Go(func() {
			for i := range perGoroutine {
				_, _ = h.Do(context.Background(), func(context.Context) (int, error) {
					// Three consecutive failures per burst, so each goroutine
					// can trip the circuit on its own under any schedule,
					// including GOMAXPROCS=1 where it may run unpreempted.
					if (g+i)%8 < 3 {
						return 0, errBoom
					}
					return 0, nil
				})
				if i%5 == 0 {
					h.clock.Add(time.Millisecond) // let the circuit recover now and then
				}
			}
		})
	}
	wg.Wait()
	s := h.Stats()
	if s.Calls != goroutines*perGoroutine {
		t.Fatalf("Calls = %d, want %d", s.Calls, goroutines*perGoroutine)
	}
	if s.Admitted+s.Rejected+s.Shed+s.Denied != s.Calls {
		t.Fatalf("Admitted(%d) + Rejected(%d) + Shed(%d) + Denied(%d) != Calls(%d)", s.Admitted, s.Rejected, s.Shed, s.Denied, s.Calls)
	}
	if s.Successes+s.Failures+s.Canceled != s.Admitted {
		t.Fatalf("Successes(%d) + Failures(%d) + Canceled(%d) != Admitted(%d)", s.Successes, s.Failures, s.Canceled, s.Admitted)
	}
	if s.Trips == 0 || s.Rejected == 0 {
		t.Fatalf("test did not exercise the open path: %+v", s)
	}
}

func TestCanceledContextDuringAcquire(t *testing.T) {
	h := newHarness(t)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, err := h.Do(ctx, func(context.Context) (int, error) {
		t.Fatal("fn invoked with a cancelled context")
		return 0, nil
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
	if s := h.Stats(); s.Calls != 0 {
		t.Fatalf("a call that never reached the loop was counted: %+v", s)
	}
}

func TestStopIsIdempotentAndConcurrent(t *testing.T) {
	b, err := New(t.Name())
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for range 10 {
		wg.Go(b.Stop)
	}
	wg.Wait()
	b.Stop()

	_, err = b.Do(t.Context(), func(context.Context) (int, error) { return 1, nil })
	if !errors.Is(err, ErrStopped) {
		t.Fatalf("Do after stop: err = %v, want ErrStopped", err)
	}
	if s := b.Stats(); s != (Stats{}) {
		t.Fatalf("Stats after stop = %+v, want zero", s)
	}
}

func TestStopLetsInFlightCallsFinish(t *testing.T) {
	b, err := New(t.Name())
	if err != nil {
		t.Fatal(err)
	}
	entered := make(chan struct{})
	finish := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		v, err := b.Do(context.Background(), func(context.Context) (int, error) {
			close(entered)
			<-finish
			return 7, nil
		})
		if v != 7 {
			err = fmt.Errorf("v = %d", v)
		}
		done <- err
	}()
	recv(t, entered)
	b.Stop()
	close(finish)
	if err := recv(t, done); err != nil {
		t.Fatalf("in-flight call after Stop: %v", err)
	}
}

func TestPanicInFnReleasesProbeSlot(t *testing.T) {
	h := newHarness(t, WithFailureThreshold(1))
	h.trip(t)
	h.clock.Add(time.Second)
	func() {
		defer func() {
			if recover() == nil {
				t.Fatal("panic did not propagate")
			}
		}()
		h.Do(t.Context(), func(context.Context) (int, error) { panic("probe exploded") })
	}()
	// The panicking probe counted as a failure and reopened the circuit;
	// the slot was not leaked, so after the interval a probe is admitted.
	h.wantState(t, Open)
	h.clock.Add(2 * time.Second)
	if err := h.ok(t); err != nil {
		t.Fatalf("probe after panic: %v", err)
	}
}

func TestStatsString(t *testing.T) {
	h := newHarness(t, WithFailureThreshold(1), WithOpenInterval(23*time.Second, time.Minute))
	h.trip(t)
	h.fail(t) // rejected
	s := h.Stats().String()
	for _, want := range []string{"state=open", "trips=1(consecutive=1)", "calls=2", "rejected=1", "ok=0", "fail=1", "next_probe_in=23s"} {
		if !strings.Contains(s, want) {
			t.Errorf("%q does not contain %q", s, want)
		}
	}
	h.clock.Add(23 * time.Second)
	if s := h.Stats().String(); !strings.Contains(s, "state=half-open") || strings.Contains(s, "next_probe_in") {
		t.Errorf("half-open line = %q", s)
	}
}

func TestStateString(t *testing.T) {
	for s, want := range map[State]string{Closed: "closed", Open: "open", HalfOpen: "half-open", State(9): "State(9)"} {
		if got := s.String(); got != want {
			t.Errorf("State(%d).String() = %q, want %q", s, got, want)
		}
	}
}

func TestDefaults(t *testing.T) {
	b, err := New(t.Name())
	if err != nil {
		t.Fatal(err)
	}
	defer b.Stop()
	got := b.cfg
	if got.failureThreshold != 5 || got.successThreshold != 2 || got.maxProbes != 1 ||
		got.openBase != 5*time.Second || got.openMax != time.Minute || got.openJitter != 0.2 ||
		got.isFailure == nil || got.now == nil || got.seed == [2]uint64{} {
		t.Fatalf("defaults = %+v", got)
	}
	if !got.isFailure(errBoom) || got.isFailure(nil) || got.isFailure(context.Canceled) || !got.isFailure(context.DeadlineExceeded) {
		t.Fatal("default IsFailure misclassifies")
	}
}

func TestInvalidOptionsAreReported(t *testing.T) {
	cases := map[string]Option{
		"WithFailureThreshold(0)":  WithFailureThreshold(0),
		"WithSuccessThreshold(-1)": WithSuccessThreshold(-1),
		"WithMaxProbes(0)":         WithMaxProbes(0),
		"WithOpenInterval(0, 1s)":  WithOpenInterval(0, time.Second),
		"WithOpenInterval(2s, 1s)": WithOpenInterval(2*time.Second, time.Second),
		"WithOpenJitter(1.5)":      WithOpenJitter(1.5),
		"WithOpenJitter(-0.1)":     WithOpenJitter(-0.1),
		"WithOpenJitter(NaN)":      WithOpenJitter(math.NaN()),
		"WithIsFailure(nil)":       WithIsFailure(nil),
		"WithClock(nil)":           WithClock(nil),
	}
	for name, opt := range cases {
		t.Run(name, func(t *testing.T) {
			b, err := New(t.Name(), opt)
			if b != nil || !errors.Is(err, ErrInvalidOption) {
				t.Fatalf("New = %v, %v; want nil, ErrInvalidOption", b, err)
			}
		})
	}
}

func TestNewReportsEveryInvalidOption(t *testing.T) {
	_, err := New("x", WithFailureThreshold(0), WithMaxProbes(0), WithSuccessThreshold(3))
	if !errors.Is(err, ErrInvalidOption) {
		t.Fatalf("err = %v", err)
	}
	for _, want := range []string{"WithFailureThreshold(0)", "WithMaxProbes(0)"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("%q does not mention %s", err, want)
		}
	}
	if strings.Contains(err.Error(), "WithSuccessThreshold") {
		t.Errorf("%q mentions a valid option", err)
	}
}

// TestDoDoesNotAllocateInSteadyState pins the token free list: once a call's
// channel has been recycled, admitted and rejected calls allocate nothing.
func TestDoDoesNotAllocateInSteadyState(t *testing.T) {
	h := newHarness(t, WithFailureThreshold(1))
	ctx := context.Background()
	work := func(context.Context) (int, error) { return 1, nil }
	h.ok(t) // warm the free list
	if n := testing.AllocsPerRun(100, func() { h.Do(ctx, work) }); n != 0 {
		t.Errorf("closed Do allocates %.1f objects per call, want 0", n)
	}
	h.trip(t)
	if n := testing.AllocsPerRun(100, func() { h.Do(ctx, work) }); n != 0 {
		t.Errorf("open Do allocates %.1f objects per call, want 0", n)
	}
}

// TestTokenPoolSaturation checks that more concurrent calls than the free
// list holds is merely slower, not wrong.
func TestTokenPoolSaturation(t *testing.T) {
	h := newHarness(t)
	release := make(chan struct{})
	var wg sync.WaitGroup
	for range 2 * _tokenPoolSize {
		wg.Go(func() {
			h.Do(context.Background(), func(context.Context) (int, error) {
				<-release
				return 1, nil
			})
		})
	}
	// Wait until every call is in flight, then let them all settle.
	deadline := time.Now().Add(10 * time.Second)
	for h.Stats().InFlight < 2*_tokenPoolSize {
		if time.Now().After(deadline) {
			t.Fatal("calls were not all admitted in time")
		}
		runtime.Gosched()
	}
	close(release)
	wg.Wait()
	if s := h.Stats(); s.Successes != uint64(2*_tokenPoolSize) || len(h.tokens) != _tokenPoolSize {
		t.Fatalf("stats = %+v, pooled tokens = %d", s, len(h.tokens))
	}
	h.ok(t) // a recycled token must be empty and reusable
}

// recorder is an Observer that logs every event as a string.
type recorder struct{ events []string }

func (r *recorder) Started()        { r.events = append(r.events, "started") }
func (r *recorder) Call(res Result) { r.events = append(r.events, "call:"+res.String()) }
func (r *recorder) Transition(from, to State, trips int, openUntil time.Time) {
	r.events = append(r.events, fmt.Sprintf("%s->%s trips=%d open_until_zero=%t", from, to, trips, openUntil.IsZero()))
}
func (r *recorder) Load(inFlight, limit int, ramping bool) {
	e := fmt.Sprintf("load:%d/%d", inFlight, limit)
	if ramping {
		e += " ramping"
	}
	r.events = append(r.events, e)
}
func (r *recorder) Stopped() { r.events = append(r.events, "stopped") }

func TestObserverEventSequence(t *testing.T) {
	rec := &recorder{}
	h := newHarness(t, WithFailureThreshold(1), WithObserver(rec))
	h.fail(t)
	h.fail(t) // rejected
	h.clock.Add(time.Second)
	h.ok(t)
	h.Do(t.Context(), func(context.Context) (int, error) { return 0, context.Canceled })
	h.ok(t)
	h.Stop() // orders the read below after the loop's writes
	want := []string{
		"started",
		"load:0/0", // initial cap, once the loop is running
		"load:1/0", // admitted
		"call:failure",
		"closed->open trips=1 open_until_zero=false",
		"load:0/0", // load follows the transition so a cap it set is seen
		"call:rejected",
		"open->half-open trips=1 open_until_zero=true",
		"load:1/0",
		"call:success",
		"load:0/0",
		"load:1/0",
		"call:canceled",
		"load:0/0",
		"load:1/0",
		"call:success",
		"half-open->closed trips=0 open_until_zero=true",
		"load:0/0",
		"stopped",
	}
	if !slices.Equal(rec.events, want) {
		t.Fatalf("events:\n got %q\nwant %q", rec.events, want)
	}
}

func TestObserverSeesStaleOutcomes(t *testing.T) {
	rec := &recorder{}
	h := newHarness(t, WithFailureThreshold(1), WithObserver(rec))
	entered, finish, done := make(chan struct{}), make(chan struct{}), make(chan error, 1)
	go func() {
		_, err := h.Do(context.Background(), func(context.Context) (int, error) {
			close(entered)
			<-finish
			return 1, nil
		})
		done <- err
	}()
	recv(t, entered)
	h.trip(t)
	close(finish)
	recv(t, done)
	h.Stop()
	if n := slices.Index(rec.events, "call:success"); n < 0 {
		t.Fatalf("stale success not reported to observer: %q", rec.events)
	}
}

func TestWithObserverNil(t *testing.T) {
	if _, err := New("x", WithObserver(nil)); !errors.Is(err, ErrInvalidOption) {
		t.Fatalf("err = %v", err)
	}
}

func TestNewRequiresName(t *testing.T) {
	b, err := New("")
	if b != nil || !errors.Is(err, ErrInvalidOption) || !strings.Contains(err.Error(), "name") {
		t.Fatalf("New(\"\") = %v, %v; want nil, ErrInvalidOption mentioning the name", b, err)
	}
	// Reported alongside invalid options, not instead of them.
	_, err = New("", WithMaxProbes(0))
	if !strings.Contains(err.Error(), "name") || !strings.Contains(err.Error(), "WithMaxProbes(0)") {
		t.Fatalf("err = %v; want both problems", err)
	}
}

func TestStatsStringIncludesName(t *testing.T) {
	h := newNamedHarness(t, "db-fallback")
	if s := h.Stats().String(); !strings.HasPrefix(s, "breaker: name=db-fallback state=closed") {
		t.Fatalf("line = %q", s)
	}
}

// tally is an Observer that counts, for cross-checking against Stats.
type tally struct {
	calls map[Result]uint64
	trips uint64
}

func (c *tally) Started()            { c.calls = map[Result]uint64{} }
func (c *tally) Call(r Result)       { c.calls[r]++ }
func (c *tally) Stopped()            {}
func (c *tally) Load(int, int, bool) {}
func (c *tally) Transition(_, to State, _ int, _ time.Time) {
	if to == Open {
		c.trips++
	}
}

func TestWithTimeoutTripsOnHungBackend(t *testing.T) {
	h := newHarness(t, WithFailureThreshold(1), WithTimeout(20*time.Millisecond))
	start := time.Now()
	_, err := h.Do(context.Background(), func(ctx context.Context) (int, error) {
		<-ctx.Done() // a backend that never answers, but honours cancellation
		return 0, ctx.Err()
	})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v, want DeadlineExceeded", err)
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("timeout took %s", elapsed)
	}
	h.wantState(t, Open)
}

func TestWithTimeoutCallerDeadlineWins(t *testing.T) {
	h := newHarness(t, WithTimeout(time.Hour))
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	var seen time.Time
	h.Do(ctx, func(ctx context.Context) (int, error) {
		seen, _ = ctx.Deadline()
		return 1, nil
	})
	if seen.IsZero() || time.Until(seen) > time.Minute {
		t.Fatalf("fn saw deadline %v; want the caller's 10ms one", seen)
	}
}

func TestWithTimeoutNotAppliedByDefault(t *testing.T) {
	h := newHarness(t)
	h.Do(context.Background(), func(ctx context.Context) (int, error) {
		if _, ok := ctx.Deadline(); ok {
			t.Error("fn received a deadline without WithTimeout")
		}
		return 1, nil
	})
}

func TestWithTimeoutInvalid(t *testing.T) {
	for _, d := range []time.Duration{0, -time.Second} {
		if _, err := New("x", WithTimeout(d)); !errors.Is(err, ErrInvalidOption) {
			t.Errorf("WithTimeout(%s): err = %v", d, err)
		}
	}
}

func TestBulkheadShedsAtCap(t *testing.T) {
	h := newHarness(t, WithMaxInFlight(2))
	entered, finish := make(chan struct{}, 2), make(chan struct{})
	done := make(chan error, 2)
	for range 2 {
		go func() {
			_, err := h.Do(context.Background(), func(context.Context) (int, error) {
				entered <- struct{}{}
				<-finish
				return 1, nil
			})
			done <- err
		}()
	}
	recv(t, entered)
	recv(t, entered)

	if err := h.ok(t); !errors.Is(err, ErrBulkhead) {
		t.Fatalf("third call: err = %v, want ErrBulkhead", err)
	}
	s := h.Stats()
	if s.InFlight != 2 || s.InFlightLimit != 2 || s.Shed != 1 || s.Rejected != 0 || s.Calls != 3 || s.Admitted != 0 {
		t.Fatalf("stats = %+v", s)
	}
	if !strings.Contains(s.String(), "shed=1") || !strings.Contains(s.String(), "in_flight=2/2") {
		t.Fatalf("line = %q", s.String())
	}
	h.wantState(t, Closed) // shedding is not a failure

	close(finish)
	recv(t, done)
	recv(t, done)
	if err := h.ok(t); err != nil {
		t.Fatalf("after release: %v", err)
	}
	if s := h.Stats(); s.InFlight != 0 || s.Admitted+s.Rejected+s.Shed != s.Calls {
		t.Fatalf("stats = %+v", s)
	}
}

func TestBulkheadOpenCircuitWins(t *testing.T) {
	h := newHarness(t, WithFailureThreshold(1), WithMaxInFlight(1))
	h.trip(t)
	if err := h.ok(t); !errors.Is(err, ErrOpen) {
		t.Fatalf("err = %v, want ErrOpen, the more informative rejection", err)
	}
}

func TestBulkheadDoesNotCountProbeRejections(t *testing.T) {
	// While half-open the probe limit applies first; a shed call must not
	// consume a probe slot either.
	h := newHarness(t, WithFailureThreshold(1), WithMaxProbes(3), WithMaxInFlight(1))
	h.trip(t)
	h.clock.Add(time.Second)
	entered, finish, done := make(chan struct{}), make(chan struct{}), make(chan error, 1)
	go func() {
		_, err := h.Do(context.Background(), func(context.Context) (int, error) {
			close(entered)
			<-finish
			return 1, nil
		})
		done <- err
	}()
	recv(t, entered)
	if err := h.ok(t); !errors.Is(err, ErrBulkhead) {
		t.Fatalf("err = %v, want ErrBulkhead", err)
	}
	close(finish)
	recv(t, done)
	// The probe slot the shed call did not take is still available.
	if err := h.ok(t); err != nil {
		t.Fatal(err)
	}
	h.wantState(t, Closed)
}

func TestBulkheadUnlimitedByDefault(t *testing.T) {
	h := newHarness(t)
	if s := h.Stats(); s.InFlightLimit != 0 || !strings.Contains(s.String(), "in_flight=0 ") && !strings.HasSuffix(s.String(), "in_flight=0") {
		t.Fatalf("stats = %+v, line = %q", s, s.String())
	}
}

// slow runs a call whose fn advances the fake clock by d, so the breaker
// measures it as having taken d.
func (h *harness) slow(t *testing.T, d time.Duration, err error) {
	t.Helper()
	h.Do(t.Context(), func(context.Context) (int, error) {
		h.clock.Add(d)
		return 0, err
	})
}

func TestAdaptiveInFlightAIMD(t *testing.T) {
	h := newHarness(t, WithAdaptiveInFlight(2, 8, 100*time.Millisecond))
	limit := func() int { return h.Stats().InFlightLimit }
	if limit() != 8 {
		t.Fatalf("initial limit = %d, want max (8)", limit())
	}
	h.slow(t, 200*time.Millisecond, nil) // slow success halves
	if limit() != 4 {
		t.Fatalf("after slow call: %d, want 4", limit())
	}
	h.slow(t, time.Millisecond, errBoom) // failure halves
	if limit() != 2 {
		t.Fatalf("after failure: %d, want 2", limit())
	}
	h.slow(t, time.Millisecond, errBoom) // floor
	if limit() != 2 {
		t.Fatalf("below min: %d", limit())
	}
	h.slow(t, time.Millisecond, context.Canceled) // neutral
	if limit() != 2 {
		t.Fatalf("cancellation moved the limit: %d", limit())
	}
	for i := range 10 {
		h.slow(t, 50*time.Millisecond, nil) // fast success: +1 up to max
		if want := min(3+i, 8); limit() != want {
			t.Fatalf("after %d fast successes: %d, want %d", i+1, limit(), want)
		}
	}
}

func TestAdaptiveInFlightEnforcesCurrentLimit(t *testing.T) {
	h := newHarness(t, WithFailureThreshold(100), WithAdaptiveInFlight(1, 4, 100*time.Millisecond))
	h.slow(t, time.Millisecond, errBoom) // 4 -> 2
	h.slow(t, time.Millisecond, errBoom) // 2 -> 1
	entered, finish := make(chan struct{}), make(chan struct{})
	go h.Do(context.Background(), func(context.Context) (int, error) {
		close(entered)
		<-finish
		return 1, nil
	})
	recv(t, entered)
	if err := h.ok(t); !errors.Is(err, ErrBulkhead) {
		t.Fatalf("err = %v, want ErrBulkhead at adaptive limit 1", err)
	}
	close(finish)
}

func TestBulkheadOptionsInvalid(t *testing.T) {
	cases := map[string][]Option{
		"WithMaxInFlight(0)":             {WithMaxInFlight(0)},
		"WithAdaptiveInFlight(0, 4, 1s)": {WithAdaptiveInFlight(0, 4, time.Second)},
		"WithAdaptiveInFlight(4, 2, 1s)": {WithAdaptiveInFlight(4, 2, time.Second)},
		"WithAdaptiveInFlight(1, 4, 0)":  {WithAdaptiveInFlight(1, 4, 0)},
		"both bulkheads":                 {WithMaxInFlight(4), WithAdaptiveInFlight(1, 4, time.Second)},
	}
	for name, opts := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := New("x", opts...); !errors.Is(err, ErrInvalidOption) {
				t.Fatalf("err = %v", err)
			}
		})
	}
}

func TestRecoveryRampStatic(t *testing.T) {
	h := newHarness(t, WithFailureThreshold(1), WithSuccessThreshold(1), WithMaxInFlight(4), WithRecoveryRamp(1, 3))
	if s := h.Stats(); s.Ramping || s.InFlightLimit != 4 {
		t.Fatalf("at startup: %+v", s) // no ramp before a recovery
	}
	h.trip(t)
	h.clock.Add(time.Second)
	h.ok(t) // probe closes the circuit
	s := h.Stats()
	if s.State != Closed || !s.Ramping || s.InFlightLimit != 1 || !strings.Contains(s.String(), "in_flight=0/1(ramping)") {
		t.Fatalf("after close: %+v %q", s, s.String())
	}

	// The ramp is enforced: one in flight fills the cap of 1.
	entered, finish, done := make(chan struct{}), make(chan struct{}), make(chan error, 1)
	go func() {
		_, err := h.Do(context.Background(), func(context.Context) (int, error) {
			close(entered)
			<-finish
			return 1, nil
		})
		done <- err
	}()
	recv(t, entered)
	if err := h.ok(t); !errors.Is(err, ErrBulkhead) {
		t.Fatalf("during ramp: err = %v, want ErrBulkhead", err)
	}
	close(finish)
	recv(t, done) // success: 1 -> 2
	if s := h.Stats(); !s.Ramping || s.InFlightLimit != 2 {
		t.Fatalf("after one success: %+v", s)
	}
	h.ok(t) // 2 -> 3 == end: ramp over, steady cap resumes
	if s := h.Stats(); s.Ramping || s.InFlightLimit != 4 || strings.Contains(s.String(), "ramping") {
		t.Fatalf("after ramp: %+v %q", s, s.String())
	}
}

func TestRecoveryRampToUnlimited(t *testing.T) {
	h := newHarness(t, WithFailureThreshold(1), WithSuccessThreshold(1), WithRecoveryRamp(1, 2))
	h.trip(t)
	h.clock.Add(time.Second)
	h.ok(t)
	if s := h.Stats(); !s.Ramping || s.InFlightLimit != 1 {
		t.Fatalf("after close: %+v", s)
	}
	h.ok(t) // 1 -> 2 == end
	if s := h.Stats(); s.Ramping || s.InFlightLimit != 0 {
		t.Fatalf("after ramp: %+v, want unlimited", s)
	}
}

func TestRecoveryRampAbortsOnTripAndRestarts(t *testing.T) {
	h := newHarness(t, WithFailureThreshold(2), WithSuccessThreshold(1), WithMaxInFlight(8), WithRecoveryRamp(2, 6))
	h.trip(t)
	h.clock.Add(time.Second)
	h.ok(t) // close: ramp at 2
	h.ok(t) // 3
	h.fail(t)
	h.fail(t) // trips again mid-ramp
	if s := h.Stats(); s.State != Open || s.Ramping || s.InFlightLimit != 8 {
		t.Fatalf("after re-trip: %+v", s)
	}
	h.clock.Add(2 * time.Second)
	h.ok(t) // close again: ramp restarts from start, not from 3
	if s := h.Stats(); !s.Ramping || s.InFlightLimit != 2 {
		t.Fatalf("after second close: %+v", s)
	}
}

func TestRecoveryRampFailuresDoNotGrowIt(t *testing.T) {
	h := newHarness(t, WithFailureThreshold(5), WithSuccessThreshold(1), WithRecoveryRamp(1, 4))
	h.trip(t)
	h.clock.Add(time.Second)
	h.ok(t)
	h.fail(t)
	h.Do(t.Context(), func(context.Context) (int, error) { return 0, context.Canceled })
	if s := h.Stats(); !s.Ramping || s.InFlightLimit != 1 {
		t.Fatalf("failure or cancellation moved the ramp: %+v", s)
	}
}

func TestRecoveryRampSeedsAIMD(t *testing.T) {
	h := newHarness(t, WithFailureThreshold(1), WithSuccessThreshold(1),
		WithAdaptiveInFlight(1, 8, 100*time.Millisecond), WithRecoveryRamp(2, 4))
	h.trip(t)
	h.clock.Add(time.Second)
	h.slow(t, time.Millisecond, nil) // probe closes; ramp seeds the cap at 2
	if s := h.Stats(); !s.Ramping || s.InFlightLimit != 2 {
		t.Fatalf("after close: %+v", s)
	}
	h.slow(t, time.Millisecond, nil) // AIMD: 3
	h.slow(t, time.Millisecond, nil) // AIMD: 4 == end, ramp over
	if s := h.Stats(); s.Ramping || s.InFlightLimit != 4 {
		t.Fatalf("after reaching end: %+v", s)
	}
	h.slow(t, time.Millisecond, nil) // AIMD keeps going: 5
	if s := h.Stats(); s.InFlightLimit != 5 {
		t.Fatalf("AIMD stopped growing: %+v", s)
	}

	// A decrease ends the ramp early: that is backoff, not recovery.
	h.fail(t) // trips (threshold 1); AIMD halves 5 -> 2
	h.clock.Add(2 * time.Second)
	h.slow(t, time.Millisecond, nil)     // close; ramp seeds 2
	h.slow(t, 200*time.Millisecond, nil) // slow: halve to 1, ramp over
	if s := h.Stats(); s.Ramping || s.InFlightLimit != 1 {
		t.Fatalf("after slow call during ramp: %+v", s)
	}
}

func TestRecoveryRampInvalid(t *testing.T) {
	cases := map[string][]Option{
		"start 0":          {WithRecoveryRamp(0, 2)},
		"end < start":      {WithRecoveryRamp(3, 2)},
		"end > static cap": {WithMaxInFlight(4), WithRecoveryRamp(1, 5)},
		"start < aimd min": {WithAdaptiveInFlight(2, 8, time.Second), WithRecoveryRamp(1, 4)},
		"end > aimd max":   {WithAdaptiveInFlight(1, 4, time.Second), WithRecoveryRamp(1, 5)},
	}
	for name, opts := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := New("x", opts...); !errors.Is(err, ErrInvalidOption) {
				t.Fatalf("err = %v", err)
			}
		})
	}
}

// denyKey marks a context whose call the test admission hook must veto.
type denyKey struct{}

var errDenied = errors.New("denied by test hook")

func denyIfMarked(ctx context.Context) error {
	if ctx.Value(denyKey{}) != nil {
		return errDenied
	}
	return nil
}

func TestAdmissionDeniedIsNeutralAndUnadmitted(t *testing.T) {
	h := newHarness(t, WithFailureThreshold(2), WithMaxInFlight(1), WithAdmission(denyIfMarked))
	h.fail(t) // one failure on the run
	ran := false
	_, err := h.Do(context.WithValue(t.Context(), denyKey{}, true), func(context.Context) (int, error) {
		ran = true
		return 1, nil
	})
	if !errors.Is(err, errDenied) || ran {
		t.Fatalf("err = %v, ran = %t", err, ran)
	}
	s := h.Stats()
	if s.Denied != 1 || s.Admitted != 1 || s.Calls != 2 || s.Failures != 1 || s.InFlight != 0 {
		t.Fatalf("stats = %+v", s)
	}
	if s.Admitted+s.Rejected+s.Shed+s.Denied != s.Calls {
		t.Fatalf("invariant broken: %+v", s)
	}
	if !strings.Contains(s.String(), "denied=1") {
		t.Fatalf("line = %q", s.String())
	}
	h.fail(t) // the denial did not reset the failure run: this is the second
	h.wantState(t, Open)
}

func TestAdmissionRunsAfterCircuitAndBulkhead(t *testing.T) {
	calls := 0
	h := newHarness(t, WithFailureThreshold(1), WithAdmission(func(context.Context) error { calls++; return nil }))
	h.trip(t)
	if err := h.ok(t); !errors.Is(err, ErrOpen) {
		t.Fatalf("err = %v", err)
	}
	h.Stop() // orders the read of calls after the loop
	if calls != 1 {
		t.Fatalf("hook ran %d times; the rejected call must not reach it", calls)
	}
}

func TestAdmissionDeniedProbeFreesSlot(t *testing.T) {
	h := newHarness(t, WithFailureThreshold(1), WithAdmission(denyIfMarked))
	h.trip(t)
	h.clock.Add(time.Second)
	if _, err := h.Do(context.WithValue(t.Context(), denyKey{}, true), func(context.Context) (int, error) { return 1, nil }); !errors.Is(err, errDenied) {
		t.Fatalf("err = %v", err)
	}
	h.wantState(t, HalfOpen) // a denial is not a probe result
	if err := h.ok(t); err != nil {
		t.Fatalf("probe slot leaked: %v", err)
	}
}

func TestAdmissionDoesNotMoveRampOrAIMD(t *testing.T) {
	h := newHarness(t, WithAdaptiveInFlight(1, 8, 100*time.Millisecond), WithAdmission(denyIfMarked))
	h.Do(context.WithValue(t.Context(), denyKey{}, true), func(context.Context) (int, error) { return 1, nil })
	if s := h.Stats(); s.InFlightLimit != 8 {
		t.Fatalf("denial moved the adaptive limit: %+v", s)
	}
}

func TestWithAdmissionNil(t *testing.T) {
	if _, err := New("x", WithAdmission(nil)); !errors.Is(err, ErrInvalidOption) {
		t.Fatalf("err = %v", err)
	}
}

// TestClockAdvanceRightAfterDo is the regression test for the synchronous
// settle. Each iteration trips the circuit, advances the fake clock past the
// open interval with no barrier of any kind between Do returning and the
// advance, and asserts HalfOpen, then closes it and asserts Closed.
//
// Before release waited for the loop to acknowledge the outcome, Do could
// return after the loop had *received* the failure but before it had
// *applied* it. The clock advance then landed before the trip computed its
// deadline, the deadline was computed against the advanced clock, and the
// HalfOpen assertion failed (or, with a polling assertion, hung). There must
// be no Stats call between Do and the clock advance here; that call would
// act as an accidental barrier and hide the bug.
func TestClockAdvanceRightAfterDo(t *testing.T) {
	h := newHarness(t, WithFailureThreshold(1), WithSuccessThreshold(1), WithOpenInterval(time.Second, time.Second))
	for i := range 500 {
		h.fail(t)
		h.clock.Add(time.Second) // no barrier
		if got := h.State(); got != HalfOpen {
			t.Fatalf("iteration %d: state after advance = %s, want half-open", i, got)
		}
		h.ok(t)
		if got := h.State(); got != Closed {
			t.Fatalf("iteration %d: state after probe = %s, want closed", i, got)
		}
	}
	if s := h.Stats(); s.Trips != 500 || s.ConsecutiveTrips != 0 {
		t.Fatalf("stats = %+v", s)
	}
}

// refModel is an independent, single-threaded reference implementation of
// the breaker's contract. It is deliberately written from the documentation
// rather than from breaker.go, so that TestModel compares two formulations of
// the same rules rather than the code against itself. Jitter is off because
// the model cannot predict a random draw.
type refModel struct {
	failureThreshold, successThreshold, maxProbes, maxInFlight int
	rampStart, rampEnd                                         int // 0 = no ramp
	openBase, openMax                                          time.Duration

	state            State
	gen              uint64
	consecFail       int
	consecOK         int
	probes           int
	inFlight         int
	limit            int // current cap: maxInFlight, or the ramp's
	ramping          bool
	openUntil        time.Time
	consecutiveTrips int

	calls, rejected, shed, denied, admitted, successes, failures, canceled, trips uint64
}

// deny models a call the admission veto refused after the circuit admitted
// it: the admission is taken back and nothing else moves.
func (m *refModel) deny(gen uint64) {
	m.inFlight--
	m.denied++
	if gen == m.gen && m.state == HalfOpen {
		m.probes--
	}
}

func (m *refModel) expire(now time.Time) {
	if m.state == Open && !now.Before(m.openUntil) {
		m.state = HalfOpen
		m.gen++
		m.probes = 0
		m.consecOK = 0
	}
}

func (m *refModel) admit(now time.Time) (uint64, error) {
	m.expire(now)
	m.calls++
	if m.state == Open {
		m.rejected++
		return 0, ErrOpen
	}
	if m.state == HalfOpen && m.probes == m.maxProbes {
		m.rejected++
		return 0, ErrProbeLimit
	}
	// >= rather than ==: calls admitted before a trip may still be in flight
	// when a ramp sets a cap below their number.
	if m.limit > 0 && m.inFlight >= m.limit {
		m.shed++
		return 0, ErrBulkhead
	}
	if m.state == HalfOpen {
		m.probes++
	}
	m.inFlight++
	return m.gen, nil
}

func (m *refModel) settle(gen uint64, out outcome, now time.Time) {
	m.inFlight--
	m.admitted++
	switch out {
	case _outcomeSuccess:
		m.successes++
	case _outcomeFailure:
		m.failures++
	case _outcomeCanceled:
		m.canceled++
	}
	if gen != m.gen {
		return // stale
	}
	if m.state == HalfOpen {
		m.probes--
	}
	if out == _outcomeCanceled {
		return
	}
	if out == _outcomeFailure {
		if m.state == HalfOpen || m.consecFail+1 == m.failureThreshold {
			m.trip(now)
		} else {
			m.consecFail++
		}
		return
	}
	// success
	if m.state == Closed {
		m.consecFail = 0
		if m.ramping {
			m.limit++
			if m.limit >= m.rampEnd {
				m.ramping, m.limit = false, m.maxInFlight
			}
		}
		return
	}
	m.consecOK++
	if m.consecOK == m.successThreshold {
		m.state = Closed
		m.gen++
		m.consecutiveTrips = 0
		m.consecFail = 0
		if m.rampStart > 0 {
			m.ramping, m.limit = true, m.rampStart
		}
	}
}

func (m *refModel) trip(now time.Time) {
	m.state = Open
	m.gen++
	m.trips++
	m.consecutiveTrips++
	m.consecFail = 0
	m.consecOK = 0
	m.probes = 0
	m.ramping, m.limit = false, m.maxInFlight
	d := m.openBase
	for i := 1; i < m.consecutiveTrips; i++ {
		d = min(d*2, m.openMax)
	}
	m.openUntil = now.Add(d)
}

func (m *refModel) stats(now time.Time) Stats {
	m.expire(now)
	s := Stats{
		State: m.state, ConsecutiveTrips: m.consecutiveTrips,
		InFlight: m.inFlight, InFlightLimit: m.limit, Ramping: m.ramping,
		Calls: m.calls, Rejected: m.rejected, Shed: m.shed, Denied: m.denied, Admitted: m.admitted,
		Successes: m.successes, Failures: m.failures, Canceled: m.canceled, Trips: m.trips,
	}
	if m.state == Open {
		s.NextProbeIn = m.openUntil.Sub(now)
	}
	return s
}

// inflight is a call admitted by the real breaker whose fn is blocked until
// the driver decides its outcome.
type inflight struct {
	finish chan outcome
	done   chan error
}

func outcomeErr(out outcome) error {
	switch out {
	case _outcomeSuccess:
		return nil
	case _outcomeFailure:
		return errBoom
	case _outcomeCanceled:
		return fmt.Errorf("wrapped: %w", context.Canceled)
	}
	return nil
}

// TestModel drives the breaker and the reference model with the same random
// sequence of admissions, out-of-order settles and clock advances, and
// compares Stats after every step. Failures print the seed to reproduce.
func TestModel(t *testing.T) {
	seeds, steps := 40, 400
	if testing.Short() {
		seeds, steps = 8, 200
	}
	for seed := range uint64(seeds) {
		t.Run(fmt.Sprintf("seed=%d", seed), func(t *testing.T) { runModel(t, seed, steps) })
	}
}

func runModel(t *testing.T, seed uint64, steps int) {
	rng := rand.New(rand.NewPCG(seed, 0xbeef))
	base := time.Duration(1+rng.IntN(5)) * time.Second
	m := &refModel{
		failureThreshold: 1 + rng.IntN(4),
		successThreshold: 1 + rng.IntN(3),
		maxProbes:        1 + rng.IntN(3),
		maxInFlight:      rng.IntN(5), // 0 = unlimited
		openBase:         base,
		openMax:          base * time.Duration(1+rng.IntN(8)),
	}
	opts := []Option{
		WithFailureThreshold(m.failureThreshold),
		WithSuccessThreshold(m.successThreshold),
		WithMaxProbes(m.maxProbes),
		WithOpenInterval(m.openBase, m.openMax),
	}
	m.limit = m.maxInFlight
	if m.maxInFlight > 0 {
		opts = append(opts, WithMaxInFlight(m.maxInFlight))
	}
	if rng.IntN(2) == 0 {
		ceiling := 4 // unlimited steady state: ramp anywhere in 1..4
		if m.maxInFlight > 0 {
			ceiling = m.maxInFlight // end may not exceed the static cap
		}
		m.rampStart = 1 + rng.IntN(ceiling)
		m.rampEnd = m.rampStart + rng.IntN(ceiling-m.rampStart+1)
		opts = append(opts, WithRecoveryRamp(m.rampStart, m.rampEnd))
	}
	opts = append(opts, WithAdmission(denyIfMarked))
	h := newHarness(t, opts...)
	t.Logf("seed=%d failure=%d success=%d probes=%d inflight=%d ramp=%d..%d base=%s max=%s",
		seed, m.failureThreshold, m.successThreshold, m.maxProbes, m.maxInFlight, m.rampStart, m.rampEnd, m.openBase, m.openMax)

	type pending struct {
		call *inflight
		gen  uint64 // the model's generation for this call
	}
	var live []pending

	check := func(step int, what string) {
		t.Helper()
		got, want := h.Stats(), m.stats(h.clock.Now())
		want.Name = got.Name // identity, not behaviour: the model does not carry it
		if got != want {
			t.Fatalf("step %d (%s): breaker and model diverge\n got: %+v\nwant: %+v", step, what, got, want)
		}
	}

	for step := range steps {
		switch r := rng.IntN(100); {
		case r < 50: // start a call; one in five is vetoed by the admission hook
			c := &inflight{finish: make(chan outcome), done: make(chan error, 1)}
			entered := make(chan struct{})
			ctx := context.Background()
			deny := rng.IntN(5) == 0
			if deny {
				ctx = context.WithValue(ctx, denyKey{}, true)
			}
			go func() {
				_, err := h.Do(ctx, func(context.Context) (int, error) {
					close(entered)
					return 0, outcomeErr(<-c.finish)
				})
				c.done <- err
			}()
			gen, wantErr := m.admit(h.clock.Now())
			if wantErr == nil && deny {
				m.deny(gen)
				wantErr = errDenied
			}
			select {
			case <-entered:
				if wantErr != nil {
					t.Fatalf("step %d: breaker admitted, model expected %v", step, wantErr)
				}
				live = append(live, pending{c, gen})
			case err := <-c.done:
				if !errors.Is(err, wantErr) {
					t.Fatalf("step %d: breaker returned %v, model expected %v", step, err, wantErr)
				}
			case <-time.After(10 * time.Second):
				t.Fatalf("step %d: call neither admitted nor rejected", step)
			}
			check(step, "start")
		case r < 80 && len(live) > 0: // settle a random in-flight call
			i := rng.IntN(len(live))
			p := live[i]
			live = append(live[:i], live[i+1:]...)
			out := outcome(rng.IntN(3))
			p.call.finish <- out
			recv(t, p.call.done)
			m.settle(p.gen, out, h.clock.Now())
			check(step, "settle")
		default: // advance the clock
			d := time.Duration(rng.Int64N(int64(m.openMax) * 3 / 2))
			h.clock.Add(d)
			check(step, "advance "+d.String())
		}
	}
	for _, p := range live {
		p.call.finish <- _outcomeSuccess
		recv(t, p.call.done)
		m.settle(p.gen, _outcomeSuccess, h.clock.Now())
	}
	check(steps, "drain")
}

// recv is a channel receive with a deadline, so a deadlock fails the test at
// a known line instead of hanging until the go test timeout.
func recv[T any](t *testing.T, ch <-chan T) T {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-time.After(10 * time.Second):
		t.Fatal("deadline exceeded waiting on channel")
		panic("unreachable")
	}
}

// TestChaosInvariants hammers one breaker from many goroutines with random
// outcomes, cancellations, clock advances and inspections, and checks the
// properties that must hold at every observation regardless of interleaving.
func TestChaosInvariants(t *testing.T) {
	workers, ops := 32, 400
	if testing.Short() {
		workers, ops = 8, 100
	}
	const (
		maxProbes   = 2
		maxInFlight = 8
		openMax     = 80 * time.Millisecond
		jitter      = 0.2
	)
	var transitions []State // written only by the state goroutine
	counts := &tally{}
	h := newHarness(t,
		WithFailureThreshold(3),
		WithSuccessThreshold(2),
		WithMaxProbes(maxProbes),
		WithMaxInFlight(maxInFlight),
		WithOpenInterval(10*time.Millisecond, openMax),
		WithOpenJitter(jitter),
		WithOnStateChange(func(from, to State) { transitions = append(transitions, from, to) }),
		WithObserver(counts),
		WithAdmission(denyIfMarked),
	)

	checkSnapshot := func(t *testing.T, prev, s Stats) {
		t.Helper()
		if s.Admitted+s.Rejected+s.Shed+s.Denied+uint64(s.InFlight) != s.Calls {
			t.Errorf("Admitted+Rejected+Shed+Denied+InFlight != Calls: %+v", s)
		}
		if s.InFlight < 0 || s.InFlight > maxInFlight {
			t.Errorf("InFlight out of [0, %d]: %+v", maxInFlight, s)
		}
		if s.Successes+s.Failures+s.Canceled != s.Admitted {
			t.Errorf("settled != admitted: %+v", s)
		}
		if (s.State == Open) != (s.NextProbeIn > 0) {
			t.Errorf("NextProbeIn inconsistent with state: %+v", s)
		}
		if s.NextProbeIn > time.Duration(float64(openMax)*(1+jitter))+time.Millisecond {
			t.Errorf("NextProbeIn beyond jittered OpenMax: %+v", s)
		}
		if uint64(s.ConsecutiveTrips) > s.Trips {
			t.Errorf("ConsecutiveTrips > Trips: %+v", s)
		}
		if s.Calls < prev.Calls || s.Admitted < prev.Admitted || s.Rejected < prev.Rejected || s.Shed < prev.Shed ||
			s.Successes < prev.Successes || s.Failures < prev.Failures || s.Canceled < prev.Canceled || s.Trips < prev.Trips {
			t.Errorf("a counter went backwards: %+v -> %+v", prev, s)
		}
	}

	var wg sync.WaitGroup
	for w := range workers {
		wg.Go(func() {
			rng := rand.New(rand.NewPCG(uint64(w), 42))
			var prev Stats
			for range ops {
				switch r := rng.IntN(100); {
				case r < 60:
					ctx, cancel := context.WithCancel(context.Background())
					if r < 5 {
						cancel() // acquire with a dead context
					} else if r < 12 {
						ctx = context.WithValue(ctx, denyKey{}, true) // vetoed after admission
					}
					h.Do(ctx, func(ctx context.Context) (int, error) {
						switch x := rng.IntN(10); {
						case x < 4:
							return 0, errBoom
						case x < 5:
							cancel()
							return 0, ctx.Err()
						case x < 7:
							runtime.Gosched() // let others interleave
						}
						return 1, nil
					})
					cancel()
				case r < 85:
					h.clock.Add(time.Duration(rng.Int64N(int64(openMax) / 2)))
				default:
					s := h.Stats()
					checkSnapshot(t, prev, s)
					prev = s
				}
			}
		})
	}
	wg.Wait()

	final := h.Stats()
	checkSnapshot(t, Stats{}, final)
	if final.Successes+final.Failures+final.Canceled != final.Admitted || final.InFlight != 0 {
		t.Errorf("quiescent accounting broken: %+v", final)
	}
	if final.Trips == 0 || final.Rejected == 0 || final.Canceled == 0 || final.Denied == 0 {
		t.Errorf("chaos did not exercise every path: %+v", final)
	}

	// Stop, then read the transition log and the observer's tallies: Stop
	// returns only after the state goroutine has exited, which orders these
	// reads after its writes.
	h.Stop()
	if counts.calls[Success] != final.Successes || counts.calls[Failure] != final.Failures ||
		counts.calls[Canceled] != final.Canceled || counts.calls[Rejected] != final.Rejected ||
		counts.calls[Shed] != final.Shed || counts.calls[Denied] != final.Denied || counts.trips != final.Trips {
		t.Errorf("observer tallies %v trips=%d disagree with Stats %+v", counts.calls, counts.trips, final)
	}
	legal := map[[2]State]bool{
		{Closed, Open}: true, {Open, HalfOpen}: true, {HalfOpen, Closed}: true, {HalfOpen, Open}: true,
	}
	state := Closed
	for i := 0; i+1 < len(transitions); i += 2 {
		from, to := transitions[i], transitions[i+1]
		if from != state {
			t.Fatalf("transition %d: from=%s but previous state was %s", i/2, from, state)
		}
		if !legal[[2]State{from, to}] {
			t.Fatalf("transition %d: illegal edge %s->%s", i/2, from, to)
		}
		state = to
	}
	if state != final.State {
		t.Errorf("last transition left %s, final Stats says %s", state, final.State)
	}
	if got := h.Stats(); got != (Stats{}) {
		t.Errorf("Stats after Stop = %+v", got)
	}
}

// TestDroppedBreakerStopsItself: a breaker that goes out of scope without any
// call is collected and its state goroutine exits, so creating breakers
// dynamically cannot leak.
func TestDroppedBreakerStopsItself(t *testing.T) {
	before := runtime.NumGoroutine()
	createAndDrop(t, 200)
	deadline := time.Now().Add(10 * time.Second)
	for runtime.NumGoroutine() > before {
		if time.Now().After(deadline) {
			t.Fatalf("%d goroutines before, %d after dropping 200 breakers", before, runtime.NumGoroutine())
		}
		runtime.GC()
		time.Sleep(10 * time.Millisecond)
	}
}

// createAndDrop is a separate function so nothing in the caller's frame can
// keep the breakers reachable.
func createAndDrop(t *testing.T, n int) {
	t.Helper()
	for range n {
		b, err := New("dropped")
		if err != nil {
			t.Fatal(err)
		}
		b.Do(context.Background(), func(context.Context) (int, error) { return 1, nil })
	}
}

var _sink int

func work(context.Context) (int, error) { return 1, nil }

// BenchmarkBaseline is the cost of calling fn with no breaker at all; the
// other numbers should be read as deltas from it.
func BenchmarkBaseline(b *testing.B) {
	b.ReportAllocs()
	ctx := context.Background()
	for b.Loop() {
		v, _ := work(ctx)
		_sink += v
	}
}

func BenchmarkDoClosed(b *testing.B) {
	b.ReportAllocs()
	br, err := New("bench")
	if err != nil {
		b.Fatal(err)
	}
	defer br.Stop()
	ctx := context.Background()
	for b.Loop() {
		v, _ := br.Do(ctx, work)
		_sink += v
	}
}

// BenchmarkDoClosedTimeout is BenchmarkDoClosed with WithTimeout set; the
// difference is the cost of deriving a deadline context per call.
func BenchmarkDoClosedTimeout(b *testing.B) {
	b.ReportAllocs()
	br, err := New("bench", WithTimeout(time.Second))
	if err != nil {
		b.Fatal(err)
	}
	defer br.Stop()
	ctx := context.Background()
	for b.Loop() {
		v, _ := br.Do(ctx, work)
		_sink += v
	}
}

// BenchmarkDoOpen measures the rejection path. It must be cheaper than
// BenchmarkDoClosed: it makes one channel round trip instead of two.
func BenchmarkDoOpen(b *testing.B) {
	b.ReportAllocs()
	clock := newFakeClock()
	br, err := New("bench", WithFailureThreshold(1), WithOpenInterval(time.Hour, time.Hour), WithClock(clock.Now))
	if err != nil {
		b.Fatal(err)
	}
	defer br.Stop()
	ctx := context.Background()
	br.Do(ctx, func(context.Context) (int, error) { return 0, errBoom })
	if br.State() != Open {
		b.Fatal("breaker not open")
	}
	for b.Loop() {
		v, _ := br.Do(ctx, work)
		_sink += v
	}
}

func BenchmarkDoClosedParallel(b *testing.B) {
	b.ReportAllocs()
	br, err := New("bench")
	if err != nil {
		b.Fatal(err)
	}
	defer br.Stop()
	ctx := context.Background()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			br.Do(ctx, work)
		}
	})
}

func BenchmarkStats(b *testing.B) {
	b.ReportAllocs()
	br, err := New("bench")
	if err != nil {
		b.Fatal(err)
	}
	defer br.Stop()
	for b.Loop() {
		_sink += int(br.Stats().Calls)
	}
}
