package ratelimit

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/mgiaccone/keel/breaker"
)

type fakeClock struct{ ns atomic.Int64 }

func newFakeClock() *fakeClock {
	c := &fakeClock{}
	c.ns.Store(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC).UnixNano()) // a whole minute
	return c
}
func (c *fakeClock) Now() time.Time      { return time.Unix(0, c.ns.Load()) }
func (c *fakeClock) Add(d time.Duration) { c.ns.Add(int64(d)) }

// harness is a limiter on a memory store driven by a fake clock.
type harness struct {
	*Limiter
	store *MemoryStore
	clock *fakeClock
}

func newHarness(t *testing.T, algo Algorithm, storeOpts ...MemoryOption) *harness {
	t.Helper()
	clock := newFakeClock()
	store, err := NewMemoryStore(append([]MemoryOption{WithClock(clock.Now)}, storeOpts...)...)
	if err != nil {
		t.Fatal(err)
	}
	l, err := New(t.Name(), algo, store)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(l.Stop)
	return &harness{Limiter: l, store: store, clock: clock}
}

func (h *harness) allow(t *testing.T, key string) Decision {
	t.Helper()
	d, err := h.Allow(context.Background(), key)
	if err != nil {
		t.Fatal(err)
	}
	return d
}

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

func TestSlidingWindowRetryAfterIsHonest(t *testing.T) {
	w := SlidingWindow(4, time.Minute)
	base := time.Unix(1_700_000_040, 0)
	var s State
	for range 4 {
		s, _ = w.Step(s, base)
	}
	_, d := w.Step(s, base)
	if d.Allowed || d.RetryAfter <= 0 {
		t.Fatalf("%+v", d)
	}
	if _, d2 := w.Step(s, base.Add(d.RetryAfter-time.Millisecond)); d2.Allowed {
		t.Fatalf("allowed before RetryAfter elapsed: %+v", d2)
	}
	if _, d3 := w.Step(s, base.Add(d.RetryAfter)); !d3.Allowed {
		t.Fatalf("still limited after RetryAfter: %+v", d3)
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

func TestMemoryStoreEvictsLRU(t *testing.T) {
	h := newHarness(t, GCRA(1, 1), WithMaxKeys(2))
	h.allow(t, "a")
	h.allow(t, "b")
	h.allow(t, "a") // a most recent
	h.allow(t, "c") // evicts b
	if h.store.Keys() != 2 {
		t.Fatalf("keys = %d", h.store.Keys())
	}
	if d := h.allow(t, "b"); !d.Allowed {
		t.Fatal("evicted key should return as fresh")
	}
	if d := h.allow(t, "c"); d.Allowed {
		t.Fatal("c was kept and should still be exhausted")
	}
}

func TestMemoryStoreInvalidOptions(t *testing.T) {
	if _, err := NewMemoryStore(WithMaxKeys(0)); !errors.Is(err, ErrInvalidOption) {
		t.Fatal(err)
	}
	if _, err := NewMemoryStore(WithClock(nil)); !errors.Is(err, ErrInvalidOption) {
		t.Fatal(err)
	}
}

func TestLimiterEnforcesEveryAlgorithm(t *testing.T) {
	for name, algo := range map[string]Algorithm{
		"gcra":           GCRA(1, 5),
		"fixed_window":   FixedWindow(5, time.Minute),
		"sliding_window": SlidingWindow(5, time.Minute),
	} {
		t.Run(name, func(t *testing.T) {
			h := newHarness(t, algo)
			for i := range 5 {
				if d := h.allow(t, "k"); !d.Allowed || d.Remaining != 4-i {
					t.Fatalf("call %d: %+v", i, d)
				}
			}
			if d := h.allow(t, "k"); d.Allowed || d.RetryAfter <= 0 {
				t.Fatalf("sixth: %+v", d)
			}
			if d := h.allow(t, "other"); !d.Allowed {
				t.Fatal("keys must be independent")
			}
			if s := h.Stats(); s.Algorithm != name || s.Allowed != 6 || s.Limited != 1 || s.Keys != 2 || s.Conflicts != 0 {
				t.Fatalf("stats = %+v", s)
			}
		})
	}
}

func TestLimiterRefillsAgainstTheStoreClock(t *testing.T) {
	h := newHarness(t, GCRA(10, 3))
	for range 3 {
		h.allow(t, "k")
	}
	if d := h.allow(t, "k"); d.Allowed {
		t.Fatal(d)
	}
	h.clock.Add(100 * time.Millisecond)
	if d := h.allow(t, "k"); !d.Allowed || d.Remaining != 0 {
		t.Fatalf("after refill: %+v", d)
	}
}

func TestLimiterRefusalDoesNotWrite(t *testing.T) {
	h := newHarness(t, GCRA(1, 1))
	h.allow(t, "k")
	rec, _, _ := h.store.Get(context.Background(), "k")
	h.allow(t, "k") // refused
	rec2, _, _ := h.store.Get(context.Background(), "k")
	if rec2.Version != rec.Version {
		t.Fatalf("refusal wrote: version %d -> %d", rec.Version, rec2.Version)
	}
}

// casOnly hides a store's Updater so the limiter takes the compare-and-set path.
type casOnly struct{ s Store }

func (c casOnly) Get(ctx context.Context, key string) (Record, time.Time, error) {
	return c.s.Get(ctx, key)
}
func (c casOnly) CompareAndSet(ctx context.Context, key string, expect uint64, st State, ttl time.Duration) (bool, error) {
	return c.s.CompareAndSet(ctx, key, expect, st, ttl)
}

// conflictingStore wraps a CAS-only store and fails the first n compare-and-sets.
type conflictingStore struct {
	Store
	remaining atomic.Int32
}

func (c *conflictingStore) CompareAndSet(ctx context.Context, key string, expect uint64, st State, ttl time.Duration) (bool, error) {
	if c.remaining.Add(-1) >= 0 {
		return false, nil
	}
	return c.Store.CompareAndSet(ctx, key, expect, st, ttl)
}

func TestLimiterRetriesConflictsThenGivesUp(t *testing.T) {
	mem, _ := NewMemoryStore()
	cs := &conflictingStore{Store: casOnly{mem}}
	cs.remaining.Store(2)
	l, err := New("c", GCRA(1, 5), cs, WithMaxAttempts(3))
	if err != nil {
		t.Fatal(err)
	}
	defer l.Stop()
	if d, err := l.Allow(context.Background(), "k"); err != nil || !d.Allowed {
		t.Fatalf("two conflicts within the budget: %+v %v", d, err)
	}
	if s := l.Stats(); s.Conflicts != 2 || s.Allowed != 1 {
		t.Fatalf("stats = %+v", s)
	}
	cs.remaining.Store(3)
	if _, err := l.Allow(context.Background(), "k"); !errors.Is(err, ErrContention) {
		t.Fatalf("err = %v, want ErrContention", err)
	}
	if s := l.Stats(); s.Errors != 1 || s.Conflicts != 5 {
		t.Fatalf("stats = %+v", s)
	}
}

// failingStore errors on everything.
type failingStore struct{ err error }

func (f failingStore) Get(context.Context, string) (Record, time.Time, error) {
	return Record{}, time.Time{}, f.err
}
func (f failingStore) CompareAndSet(context.Context, string, uint64, State, time.Duration) (bool, error) {
	return false, f.err
}

func TestLimiterStoreErrorsAreNotDecisions(t *testing.T) {
	reg := prometheus.NewPedanticRegistry()
	if err := Register(reg); err != nil {
		t.Fatal(err)
	}
	boom := errors.New("connection refused")
	l, err := New("err", GCRA(1, 1), failingStore{boom})
	if err != nil {
		t.Fatal(err)
	}
	defer l.Stop()
	if _, err := l.Allow(context.Background(), ""); !errors.Is(err, boom) {
		t.Fatalf("err = %v", err)
	}
	if s := l.Stats(); s.Errors != 1 || s.Allowed != 0 || s.Limited != 0 {
		t.Fatalf("stats = %+v", s)
	}
	if v := testutil.ToFloat64(_decisionsCounter.WithLabelValues("err", "gcra", "error")); v != 1 {
		t.Fatalf("error metric = %v", v)
	}
}

func TestLimiterInvalidOptions(t *testing.T) {
	mem, _ := NewMemoryStore()
	cases := map[string]func() (*Limiter, error){
		"empty name":    func() (*Limiter, error) { return New("", GCRA(1, 1), mem) },
		"nil algorithm": func() (*Limiter, error) { return New("x", nil, mem) },
		"bad algorithm": func() (*Limiter, error) { return New("x", GCRA(0, 0), mem) },
		"nil store":     func() (*Limiter, error) { return New("x", GCRA(1, 1), nil) },
		"attempts 0":    func() (*Limiter, error) { return New("x", GCRA(1, 1), mem, WithMaxAttempts(0)) },
		"several":       func() (*Limiter, error) { return New("", nil, nil, WithMaxAttempts(0)) },
	}
	for name, mk := range cases {
		t.Run(name, func(t *testing.T) {
			if l, err := mk(); l != nil || !errors.Is(err, ErrInvalidOption) {
				t.Fatalf("= %v, %v", l, err)
			}
		})
	}
}

func TestMemoryStoreNeverConflicts(t *testing.T) {
	h := newHarness(t, GCRA(1000, 5))
	var wg sync.WaitGroup
	for range 32 {
		wg.Go(func() {
			for range 50 {
				h.allow(t, "hot")
			}
		})
	}
	wg.Wait()
	if s := h.Stats(); s.Conflicts != 0 || s.Errors != 0 || s.Allowed != 5 || s.Limited != 32*50-5 {
		t.Fatalf("stats = %+v", s)
	}
}

func TestCASPathUnderContention(t *testing.T) {
	// The compare-and-set path, as a Redis store would take it: races are
	// real, retried, and never lose an update.
	clock := newFakeClock()
	mem, _ := NewMemoryStore(WithClock(clock.Now))
	l, err := New("cas", GCRA(1000, 40), casOnly{mem}, WithMaxAttempts(1000))
	if err != nil {
		t.Fatal(err)
	}
	defer l.Stop()
	var wg sync.WaitGroup
	var allowed atomic.Uint64
	for range 16 {
		wg.Go(func() {
			for range 20 {
				d, err := l.Allow(context.Background(), "hot")
				if err != nil {
					t.Error(err)
					return
				}
				if d.Allowed {
					allowed.Add(1)
				}
			}
		})
	}
	wg.Wait()
	s := l.Stats()
	if allowed.Load() != 40 || s.Allowed != 40 || s.Limited != 320-40 || s.Errors != 0 {
		t.Fatalf("stats = %+v, allowed = %d; a lost update would show as more than 40 allowed", s, allowed.Load())
	}
	t.Logf("conflicts retried: %d", s.Conflicts)
}

func TestLimiterConcurrentAccounting(t *testing.T) {
	h := newHarness(t, GCRA(1000, 50))
	var wg sync.WaitGroup
	var allowed atomic.Uint64
	for range 20 {
		wg.Go(func() {
			for i := range 50 {
				if d := h.allow(t, fmt.Sprint(i%4)); d.Allowed {
					allowed.Add(1)
				}
			}
		})
	}
	wg.Wait()
	s := h.Stats()
	if s.Allowed != allowed.Load() || s.Allowed+s.Limited != 1000 || s.Keys != 4 {
		t.Fatalf("stats = %+v, allowed seen = %d", s, allowed.Load())
	}
	// The clock is frozen, so exactly burst tokens per key were available.
	if s.Allowed != 4*50 {
		t.Fatalf("allowed = %d, want 4 keys × burst 50", s.Allowed)
	}
}

func TestStatsString(t *testing.T) {
	h := newHarness(t, GCRA(1, 1))
	h.allow(t, "k")
	h.allow(t, "k")
	want := fmt.Sprintf("ratelimit: name=%s algorithm=gcra keys=1 allowed=1 limited=1 errors=0 conflicts=0", t.Name())
	if got := h.Stats().String(); got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
}

func TestLimitedErrorIs(t *testing.T) {
	err := error(&LimitedError{Key: "acme", RetryAfter: time.Second})
	if !errors.Is(err, ErrLimited) || !strings.Contains(err.Error(), "acme") {
		t.Fatalf("err = %v", err)
	}
}

func TestAdmissionDeniesThroughBreaker(t *testing.T) {
	h := newHarness(t, GCRA(1, 2))
	b, err := breaker.New("db", breaker.WithAdmission(Admission(h.Limiter, "")))
	if err != nil {
		t.Fatal(err)
	}
	defer b.Stop()
	ctx := context.Background()
	ran := 0
	fn := func(context.Context) (int, error) { ran++; return 1, nil }
	for range 2 {
		if _, err := b.Do(ctx, fn); err != nil {
			t.Fatal(err)
		}
	}
	_, err = b.Do(ctx, fn)
	var lim *LimitedError
	if !errors.Is(err, ErrLimited) || !errors.As(err, &lim) || lim.RetryAfter != time.Second {
		t.Fatalf("err = %v", err)
	}
	if ran != 2 {
		t.Fatalf("fn ran %d times, want 2", ran)
	}
	if s := b.Stats(); s.Denied != 1 || s.Admitted != 2 || s.Calls != 3 || s.Failures != 0 || !strings.Contains(s.String(), "denied=1") {
		t.Fatalf("breaker stats = %+v", s)
	}
}

func TestAdmissionOpenCircuitDoesNotConsumeQuota(t *testing.T) {
	h := newHarness(t, GCRA(1, 1))
	b, err := breaker.New("db", breaker.WithFailureThreshold(1), breaker.WithAdmission(Admission(h.Limiter, "")))
	if err != nil {
		t.Fatal(err)
	}
	defer b.Stop()
	ctx := context.Background()
	b.Do(ctx, func(context.Context) (int, error) { return 0, errors.New("boom") }) // trips; used 1 token
	h.clock.Add(time.Second)
	if _, err := b.Do(ctx, func(context.Context) (int, error) { return 1, nil }); !errors.Is(err, breaker.ErrOpen) {
		t.Fatalf("err = %v, want ErrOpen ahead of the limiter", err)
	}
	if s := h.Stats(); s.Allowed != 1 || s.Limited != 0 {
		t.Fatalf("the rejected call consumed quota: %+v", s)
	}
}

func TestAdmissionFailsClosedAndFailOpenInverts(t *testing.T) {
	boom := errors.New("store down")
	l, _ := New("adm", GCRA(1, 1), failingStore{boom})
	defer l.Stop()
	closed, _ := breaker.New("closed", breaker.WithAdmission(Admission(l, "")))
	defer closed.Stop()
	if _, err := closed.Do(context.Background(), func(context.Context) (int, error) { return 1, nil }); !errors.Is(err, boom) {
		t.Fatalf("fail closed: err = %v", err)
	}
	var seen error
	open, _ := breaker.New("open", breaker.WithAdmission(Admission(FailOpen(l, func(err error) { seen = err }), "")))
	defer open.Stop()
	if _, err := open.Do(context.Background(), func(context.Context) (int, error) { return 1, nil }); err != nil {
		t.Fatalf("fail open: err = %v", err)
	}
	if !errors.Is(seen, boom) {
		t.Fatalf("onError saw %v", seen)
	}
}

func TestMetrics(t *testing.T) {
	reg := prometheus.NewPedanticRegistry()
	if err := Register(reg); err != nil {
		t.Fatal(err)
	}
	if err := Register(reg); err == nil {
		t.Fatal("second Register should fail")
	}
	store, _ := NewMemoryStore()
	l, err := New("m", GCRA(1, 1), store)
	if err != nil {
		t.Fatal(err)
	}
	l.Allow(context.Background(), "a")
	l.Allow(context.Background(), "a")
	l.Allow(context.Background(), "b")
	want := `
# HELP go_ratelimit_cas_conflicts_total Compare-and-set retries: two writers raced on one key. Sustained conflicts mean a hot key.
# TYPE go_ratelimit_cas_conflicts_total counter
go_ratelimit_cas_conflicts_total{algorithm="gcra",limiter="m"} 0
# HELP go_ratelimit_decisions_total Rate limiter decisions, by result. error is a decision that could not be made, e.g. Redis unreachable.
# TYPE go_ratelimit_decisions_total counter
go_ratelimit_decisions_total{algorithm="gcra",limiter="m",result="allowed"} 2
go_ratelimit_decisions_total{algorithm="gcra",limiter="m",result="error"} 0
go_ratelimit_decisions_total{algorithm="gcra",limiter="m",result="limited"} 1
# HELP go_ratelimit_keys Records the limiter's store currently holds, when the store can report it.
# TYPE go_ratelimit_keys gauge
go_ratelimit_keys{algorithm="gcra",limiter="m"} 2
`
	if err := testutil.GatherAndCompare(reg, strings.NewReader(want), "go_ratelimit_cas_conflicts_total", "go_ratelimit_decisions_total", "go_ratelimit_keys"); err != nil {
		t.Fatal(err)
	}
	problems, err := testutil.GatherAndLint(reg)
	if err != nil {
		t.Fatal(err)
	}
	for _, p := range problems {
		t.Errorf("lint: %s: %s", p.Metric, p.Text)
	}
	l.Stop()
	if n, _ := testutil.GatherAndCount(reg); n != 0 {
		t.Fatalf("%d series remain after Stop", n)
	}
	if err := Register(prometheus.NewPedanticRegistry(), WithNamespace("1bad")); !errors.Is(err, ErrInvalidOption) {
		t.Fatalf("bad namespace: %v", err)
	}
}
