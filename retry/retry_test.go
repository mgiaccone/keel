package retry

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mgiaccone/keel/breaker"
	"github.com/mgiaccone/keel/ratelimit"
)

var _nameSeq atomic.Uint64

// uniqueName gives a retrier a name no other run in this process has used, so
// tests asserting absolute metric values are not confused by -count.
func uniqueName(t *testing.T) string {
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

var (
	errBoom      = errors.New("boom")
	errTransient = errors.New("connection reset")
)

// harness is a retrier whose sleep records the waits and returns at once, on
// a fake clock, with a fixed seed.
type harness struct {
	*Retrier
	clock *fakeClock

	mu    sync.Mutex
	waits []time.Duration
	deny  atomic.Bool // makes the recorded sleep fail, as a cancelled context would
}

func newHarness(t *testing.T, backoff Backoff, opts ...Option) *harness {
	t.Helper()
	h := &harness{clock: newFakeClock()}
	base := []Option{
		WithClock(h.clock.Now),
		WithSeed(1, 2),
		WithSleep(func(ctx context.Context, d time.Duration) error {
			h.mu.Lock()
			h.waits = append(h.waits, d)
			h.mu.Unlock()
			if h.deny.Load() {
				return context.Canceled
			}
			return ctx.Err()
		}),
	}
	r, err := New(uniqueName(t), backoff, append(base, opts...)...)
	if err != nil {
		t.Fatal(err)
	}
	h.Retrier = r
	return h
}

func (h *harness) recorded() []time.Duration {
	h.mu.Lock()
	defer h.mu.Unlock()
	return slices.Clone(h.waits)
}

// failing returns fn failing with err for the first n calls, then returning v.
func failing(n int, err error, v int) func(context.Context) (int, error) {
	calls := 0
	return func(context.Context) (int, error) {
		calls++
		if calls <= n {
			return 0, err
		}
		return v, nil
	}
}

func TestSucceedsFirstTry(t *testing.T) {
	h := newHarness(t, Constant(10*time.Millisecond))
	v, err := h.Do(t.Context(), failing(0, errBoom, 42))
	if err != nil || v != 42 {
		t.Fatalf("Do = %d, %v", v, err)
	}
	if s := h.Stats(); s.Calls != 1 || s.Attempts != 1 || s.Succeeded != 1 || len(h.recorded()) != 0 {
		t.Fatalf("stats = %+v, waits = %v", s, h.recorded())
	}
}

func TestRetriesThenSucceeds(t *testing.T) {
	h := newHarness(t, Constant(10*time.Millisecond), WithMaxAttempts(5))
	v, err := h.Do(t.Context(), failing(2, errTransient, 7))
	if err != nil || v != 7 {
		t.Fatalf("Do = %d, %v", v, err)
	}
	if got := h.recorded(); !slices.Equal(got, []time.Duration{10 * time.Millisecond, 10 * time.Millisecond}) {
		t.Fatalf("waits = %v", got)
	}
	if s := h.Stats(); s.Attempts != 3 || s.Succeeded != 1 || s.Waited != 20*time.Millisecond {
		t.Fatalf("stats = %+v", s)
	}
}

func TestExhaustedReturnsLastErrorUnchanged(t *testing.T) {
	h := newHarness(t, Constant(time.Millisecond))
	errs := []error{errors.New("first"), errors.New("second"), errors.New("third")}
	i := 0
	v, err := h.Do(t.Context(), func(context.Context) (string, error) {
		i++
		return fmt.Sprint("value ", i), errs[i-1]
	})
	if err != errs[2] || v != "value 3" {
		t.Fatalf("Do = %q, %v; want the third attempt's value and error, unwrapped", v, err)
	}
	if s := h.Stats(); s.Exhausted != 1 || s.Attempts != 3 || len(h.recorded()) != 2 {
		t.Fatalf("stats = %+v, waits = %v", s, h.recorded())
	}
}

func TestPermanentStopsAndUnwraps(t *testing.T) {
	h := newHarness(t, Constant(time.Millisecond))
	_, err := h.Do(t.Context(), func(context.Context) (int, error) { return 0, Permanent(errBoom) })
	if err != errBoom {
		t.Fatalf("err = %#v, want errBoom itself", err)
	}
	if s := h.Stats(); s.Aborted != 1 || s.Attempts != 1 || len(h.recorded()) != 0 {
		t.Fatalf("stats = %+v", s)
	}
	// Wrapped deeper, it still stops the retrier; the wrapper is the caller's.
	_, err = h.Do(t.Context(), func(context.Context) (int, error) { return 0, fmt.Errorf("query: %w", Permanent(errBoom)) })
	if !errors.Is(err, errBoom) || !strings.HasPrefix(err.Error(), "query: ") {
		t.Fatalf("err = %v", err)
	}
	if s := h.Stats(); s.Aborted != 2 || s.Attempts != 2 {
		t.Fatalf("stats = %+v", s)
	}
	if Permanent(nil) != nil {
		t.Fatal("Permanent(nil) != nil")
	}
}

type decides struct{ retryable bool }

func (d decides) Error() string   { return "decides" }
func (d decides) Retryable() bool { return d.retryable }

func TestRetryableErrorDecidesOverThePredicate(t *testing.T) {
	// The predicate says never; an error that says yes is still retried.
	h := newHarness(t, Constant(time.Millisecond), WithRetryIf(func(error) bool { return false }))
	h.Do(t.Context(), failing(1, decides{true}, 1))
	if s := h.Stats(); s.Attempts != 2 || s.Succeeded != 1 {
		t.Fatalf("Retryable() true not honoured: %+v", s)
	}
	// The predicate says always; an error that says no stops.
	h = newHarness(t, Constant(time.Millisecond))
	h.Do(t.Context(), failing(1, decides{false}, 1))
	if s := h.Stats(); s.Attempts != 1 || s.Aborted != 1 {
		t.Fatalf("Retryable() false not honoured: %+v", s)
	}
}

func TestRetryIfPredicate(t *testing.T) {
	h := newHarness(t, Constant(time.Millisecond), WithRetryIf(func(err error) bool { return errors.Is(err, errTransient) }))
	if _, err := h.Do(t.Context(), failing(1, errTransient, 1)); err != nil {
		t.Fatal(err)
	}
	if _, err := h.Do(t.Context(), failing(1, errBoom, 1)); err != errBoom {
		t.Fatalf("err = %v", err)
	}
	if s := h.Stats(); s.Attempts != 3 || s.Succeeded != 1 || s.Aborted != 1 {
		t.Fatalf("stats = %+v", s)
	}
}

func TestContextDoneBeforeFirstAttempt(t *testing.T) {
	h := newHarness(t, Constant(time.Millisecond))
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, err := h.Do(ctx, func(context.Context) (int, error) {
		t.Fatal("fn ran with a done context")
		return 0, nil
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v", err)
	}
	if s := h.Stats(); s.Canceled != 1 || s.Attempts != 0 || s.Calls != 1 {
		t.Fatalf("stats = %+v", s)
	}
}

func TestContextDoneDuringCallOrWait(t *testing.T) {
	// fn cancels the context, then fails: no retry, the last error comes back.
	h := newHarness(t, Constant(time.Millisecond))
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	_, err := h.Do(ctx, func(context.Context) (int, error) { cancel(); return 0, errBoom })
	if err != errBoom {
		t.Fatalf("err = %v", err)
	}
	if s := h.Stats(); s.Canceled != 1 || s.Attempts != 1 || len(h.recorded()) != 0 {
		t.Fatalf("stats = %+v", s)
	}
	// The wait is interrupted: same outcome, one wait recorded.
	h = newHarness(t, Constant(time.Millisecond))
	h.deny.Store(true)
	if _, err := h.Do(t.Context(), failing(5, errBoom, 1)); err != errBoom {
		t.Fatalf("err = %v", err)
	}
	if s := h.Stats(); s.Canceled != 1 || s.Attempts != 1 || len(h.recorded()) != 1 {
		t.Fatalf("stats = %+v, waits = %v", s, h.recorded())
	}
}

type delayed struct{ d time.Duration }

func (d delayed) Error() string             { return "not yet" }
func (d delayed) RetryDelay() time.Duration { return d.d }

func TestDelayedErrorIsAFloorUnderTheSchedule(t *testing.T) {
	h := newHarness(t, Constant(10*time.Millisecond))
	h.Do(t.Context(), failing(1, delayed{50 * time.Millisecond}, 1))
	if got := h.recorded(); !slices.Equal(got, []time.Duration{60 * time.Millisecond}) {
		t.Fatalf("waits = %v, want the delay plus the schedule", got)
	}
	if s := h.Stats(); s.Waited != 60*time.Millisecond {
		t.Fatalf("stats = %+v", s)
	}
}

func TestDelayAboveCapIsTerminal(t *testing.T) {
	h := newHarness(t, Constant(time.Millisecond), WithMaxRetryAfter(20*time.Millisecond))
	err := delayed{time.Hour}
	if _, got := h.Do(t.Context(), failing(1, err, 1)); got != error(err) {
		t.Fatalf("err = %v", got)
	}
	if s := h.Stats(); s.Aborted != 1 || s.Attempts != 1 || len(h.recorded()) != 0 {
		t.Fatalf("stats = %+v, waits = %v", s, h.recorded())
	}
	// A negative delay is treated as none.
	h.Do(t.Context(), failing(1, delayed{-time.Second}, 1))
	if got := h.recorded(); !slices.Equal(got, []time.Duration{time.Millisecond}) {
		t.Fatalf("waits = %v", got)
	}
}

func TestBudgetDeniesRetry(t *testing.T) {
	errNoBudget := errors.New("no budget")
	asked := 0
	var hooked []string
	h := newHarness(t, Constant(time.Millisecond),
		WithMaxAttempts(5),
		WithBudget(func(context.Context) error {
			asked++
			if asked > 1 {
				return errNoBudget
			}
			return nil
		}),
		WithOnRetry(func(attempt int, err error, delay time.Duration) {
			hooked = append(hooked, fmt.Sprintf("%d:%v:%s", attempt, err, delay))
		}),
	)
	_, err := h.Do(t.Context(), failing(5, errBoom, 1))
	if err != errBoom {
		t.Fatalf("err = %v, want the last error from fn, not the budget's", err)
	}
	if s := h.Stats(); s.BudgetDenied != 1 || s.Attempts != 2 || len(h.recorded()) != 1 {
		t.Fatalf("stats = %+v, waits = %v", s, h.recorded())
	}
	want := []string{"1:boom:1ms", "2:no budget:0s"}
	if !slices.Equal(hooked, want) {
		t.Fatalf("hook saw %v, want %v", hooked, want)
	}
	// Never asked before the first attempt.
	h.Do(t.Context(), failing(0, errBoom, 1))
	if asked != 2 {
		t.Fatalf("budget asked %d times; a first attempt must not ask", asked)
	}
}

func TestInvalidOptionsAreAllReported(t *testing.T) {
	cases := map[string][]any{
		"empty name":             {"", Constant(0)},
		"nil backoff":            {"x", nil},
		"bad backoff":            {"x", Exponential(0, 0)},
		"WithMaxAttempts(0)":     {"x", Constant(0), WithMaxAttempts(0)},
		"WithRetryIf(nil)":       {"x", Constant(0), WithRetryIf(nil)},
		"WithMaxRetryAfter(0)":   {"x", Constant(0), WithMaxRetryAfter(0)},
		"WithBudget(nil)":        {"x", Constant(0), WithBudget(nil)},
		"WithObserver(nil)":      {"x", Constant(0), WithObserver(nil)},
		"WithClock(nil)":         {"x", Constant(0), WithClock(nil)},
		"WithSleep(nil)":         {"x", Constant(0), WithSleep(nil)},
		"WithOnRetry(nil) is ok": {"x", Constant(0), WithOnRetry(nil)},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			var b Backoff
			if c[1] != nil {
				b = c[1].(Backoff)
			}
			var opts []Option
			for _, o := range c[2:] {
				opts = append(opts, o.(Option))
			}
			r, err := New(c[0].(string), b, opts...)
			if strings.HasSuffix(name, "is ok") {
				if err != nil || r == nil {
					t.Fatalf("= %v, %v", r, err)
				}
				return
			}
			if r != nil || !errors.Is(err, ErrInvalidOption) {
				t.Fatalf("= %v, %v", r, err)
			}
		})
	}
	_, err := New("", Constant(-1), WithMaxAttempts(0))
	for _, want := range []string{"name", "Constant(-1ns)", "WithMaxAttempts(0)"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("%q does not mention %s", err, want)
		}
	}
}

func TestStatsIdentityUnderConcurrency(t *testing.T) {
	budgetLeft := atomic.Int64{}
	budgetLeft.Store(100)
	h := newHarness(t, Constant(time.Millisecond), WithMaxAttempts(3),
		WithBudget(func(context.Context) error {
			if budgetLeft.Add(-1) < 0 {
				return errBoom
			}
			return nil
		}))
	var wg sync.WaitGroup
	for w := range 32 {
		wg.Go(func() {
			rng := rand.New(rand.NewPCG(uint64(w), 9))
			for range 50 {
				ctx, cancel := context.WithCancel(context.Background())
				h.Do(ctx, func(context.Context) (int, error) {
					switch rng.IntN(6) {
					case 0:
						return 1, nil
					case 1:
						return 0, Permanent(errBoom)
					case 2:
						cancel()
						return 0, errBoom
					default:
						return 0, errBoom
					}
				})
				cancel()
			}
		})
	}
	wg.Wait()
	s := h.Stats()
	if s.Calls != 32*50 || s.Succeeded+s.Exhausted+s.Aborted+s.Canceled+s.BudgetDenied != s.Calls {
		t.Fatalf("identity broken: %+v", s)
	}
	if s.Attempts < s.Calls || s.Attempts > 3*s.Calls {
		t.Fatalf("attempts out of range: %+v", s)
	}
	if s.Succeeded == 0 || s.Exhausted == 0 || s.Aborted == 0 || s.Canceled == 0 || s.BudgetDenied == 0 {
		t.Fatalf("not every path exercised: %+v", s)
	}
}

func TestStatsString(t *testing.T) {
	h := newHarness(t, Exponential(time.Second, time.Minute), WithMaxAttempts(2))
	h.Do(t.Context(), failing(1, errBoom, 1))
	h.Do(t.Context(), func(context.Context) (int, error) { return 0, Permanent(errBoom) })
	got := h.Stats().String()
	want := fmt.Sprintf("retry: name=%s backoff=exponential calls=2 attempts=3 ok=1 exhausted=0 aborted=1 canceled=0 budget=0 waited=%s", h.Stats().Name, h.Stats().Waited)
	if got != want {
		t.Fatalf("got  %q\nwant %q", got, want)
	}
}

func TestResultString(t *testing.T) {
	for r, want := range map[Result]string{Success: "success", Exhausted: "exhausted", Aborted: "aborted", Canceled: "canceled", Budget: "budget", Result(9): "Result(9)"} {
		if got := r.String(); got != want {
			t.Errorf("Result(%d).String() = %q, want %q", r, got, want)
		}
	}
}

// recorder is an Observer that logs every event as a string.
type recorder struct{ events []string }

func (r *recorder) Started()      { r.events = append(r.events, "started") }
func (r *recorder) Attempt(n int) { r.events = append(r.events, fmt.Sprintf("attempt:%d", n)) }
func (r *recorder) Wait(n int, d time.Duration) {
	r.events = append(r.events, fmt.Sprintf("wait:%d/%s", n, d))
}
func (r *recorder) Call(res Result, attempts int) {
	r.events = append(r.events, fmt.Sprintf("call:%s/%d", res, attempts))
}

func TestObserverEventSequence(t *testing.T) {
	rec := &recorder{}
	h := newHarness(t, Constant(10*time.Millisecond), WithObserver(rec))
	h.Do(t.Context(), failing(1, errBoom, 1))
	h.Do(t.Context(), func(context.Context) (int, error) { return 0, Permanent(errBoom) })
	want := []string{
		"started",
		"attempt:1", "wait:1/10ms", "attempt:2", "call:success/2",
		"attempt:1", "call:aborted/1",
	}
	if !slices.Equal(rec.events, want) {
		t.Fatalf("events:\n got %q\nwant %q", rec.events, want)
	}
}

func TestSeedMakesWaitsReproducible(t *testing.T) {
	waits := func(seed uint64) []time.Duration {
		h := newHarness(t, Exponential(100*time.Millisecond, 10*time.Second), WithMaxAttempts(6), WithSeed(seed, 1))
		h.Do(t.Context(), failing(10, errBoom, 1))
		return h.recorded()
	}
	a, b, c := waits(7), waits(7), waits(8)
	if !slices.Equal(a, b) {
		t.Fatalf("same seed, different waits: %v vs %v", a, b)
	}
	if slices.Equal(a, c) {
		t.Fatalf("different seeds, same waits: %v", a)
	}
	for i, d := range a {
		if d < 0 || d >= doubled(100*time.Millisecond, 10*time.Second, i+1) {
			t.Fatalf("wait %d = %s outside the jitter range", i+1, d)
		}
	}
}

func TestBreakerRefusalIsNotRetried(t *testing.T) {
	b, err := breaker.New(uniqueName(t), breaker.WithFailureThreshold(1))
	if err != nil {
		t.Fatal(err)
	}
	defer b.Stop()
	b.Do(t.Context(), func(context.Context) (int, error) { return 0, errBoom }) // trips
	h := newHarness(t, Constant(time.Millisecond), WithMaxAttempts(5))
	ran := 0
	_, err = h.Do(t.Context(), func(ctx context.Context) (int, error) {
		return b.Do(ctx, func(context.Context) (int, error) { ran++; return 1, nil })
	})
	if !errors.Is(err, breaker.ErrOpen) || ran != 0 {
		t.Fatalf("err = %v, ran = %d", err, ran)
	}
	if s := h.Stats(); s.Attempts != 1 || s.Aborted != 1 {
		t.Fatalf("a breaker refusal was retried: %+v", s)
	}
}

func TestLimiterRefusalIsWaitedFor(t *testing.T) {
	clock := newFakeClock()
	store, _ := ratelimit.NewMemoryStore(ratelimit.WithClock(clock.Now))
	limiter, err := ratelimit.New(uniqueName(t), ratelimit.GCRA(1, 1), store)
	if err != nil {
		t.Fatal(err)
	}
	admit := ratelimit.Admission(limiter, "")
	h := newHarness(t, Constant(10*time.Millisecond), WithMaxAttempts(2))
	fn := func(ctx context.Context) (int, error) {
		if err := admit(ctx); err != nil {
			return 0, err
		}
		return 1, nil
	}
	if _, err := h.Do(t.Context(), fn); err != nil {
		t.Fatal(err) // first token
	}
	_, err = h.Do(t.Context(), fn) // refused, waited, refused again: the clock is frozen
	if !errors.Is(err, ratelimit.ErrLimited) {
		t.Fatalf("err = %v", err)
	}
	if got := h.recorded(); !slices.Equal(got, []time.Duration{time.Second + 10*time.Millisecond}) {
		t.Fatalf("waits = %v, want the limiter's 1s plus the schedule", got)
	}
	if s := h.Stats(); s.Exhausted != 1 {
		t.Fatalf("stats = %+v", s)
	}
}

func TestLimiterAsBudget(t *testing.T) {
	store, _ := ratelimit.NewMemoryStore()
	limiter, err := ratelimit.New(uniqueName(t), ratelimit.GCRA(1e-3, 2), store) // two retries, then nothing for a long time
	if err != nil {
		t.Fatal(err)
	}
	h := newHarness(t, Constant(time.Millisecond), WithMaxAttempts(10), WithBudget(ratelimit.Admission(limiter, "")))
	_, err = h.Do(t.Context(), failing(10, errBoom, 1))
	if err != errBoom {
		t.Fatalf("err = %v", err)
	}
	if s := h.Stats(); s.Attempts != 3 || s.BudgetDenied != 1 {
		t.Fatalf("stats = %+v, want two budgeted retries then a denial", s)
	}
	if s := limiter.Stats(); s.Allowed != 2 || s.Limited != 1 {
		t.Fatalf("limiter stats = %+v", s)
	}
}

var _sink int

func BenchmarkDoSuccess(b *testing.B) {
	b.ReportAllocs()
	r, err := New("bench", Exponential(time.Millisecond, time.Second))
	if err != nil {
		b.Fatal(err)
	}
	ctx := context.Background()
	ok := func(context.Context) (int, error) { return 1, nil }
	for b.Loop() {
		v, _ := r.Do(ctx, ok)
		_sink += v
	}
}

// BenchmarkDoTwoRetries is a call that fails twice and succeeds on the third
// attempt, with the wait replaced by a no-op: the cost of the retry loop
// itself, classification and bookkeeping included.
func BenchmarkDoTwoRetries(b *testing.B) {
	b.ReportAllocs()
	r, err := New("bench", Exponential(time.Millisecond, time.Second),
		WithSleep(func(context.Context, time.Duration) error { return nil }))
	if err != nil {
		b.Fatal(err)
	}
	ctx := context.Background()
	for b.Loop() {
		n := 0
		v, _ := r.Do(ctx, func(context.Context) (int, error) {
			n++
			if n < 3 {
				return 0, errBoom
			}
			return 1, nil
		})
		_sink += v
	}
}
