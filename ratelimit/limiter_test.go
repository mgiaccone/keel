package ratelimit

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

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

func TestLimiterRetriesConflictsThenGivesUp(t *testing.T) {
	mem, _ := NewMemoryStore()
	cs := &conflictingStore{Store: casOnly{mem}}
	cs.remaining.Store(2)
	l, err := New("c", GCRA(1, 5), cs, WithMaxAttempts(3))
	must(t, err)
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

func TestLimiterStoreErrorsAreNotDecisions(t *testing.T) {
	reg := prometheus.NewPedanticRegistry()
	if err := Register(reg); err != nil {
		t.Fatal(err)
	}
	boom := errors.New("connection refused")
	name := uniqueName(t)
	l, err := New(name, GCRA(1, 1), failingStore{boom})
	must(t, err)
	if _, err := l.Allow(context.Background(), ""); !errors.Is(err, boom) {
		t.Fatalf("err = %v", err)
	}
	if s := l.Stats(); s.Errors != 1 || s.Allowed != 0 || s.Limited != 0 {
		t.Fatalf("stats = %+v", s)
	}
	if v := testutil.ToFloat64(_decisionsCounter.WithLabelValues(name, "gcra", "error")); v != 1 {
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

func TestCASPathUnderContention(t *testing.T) {
	// The compare-and-set path, as a Redis store would take it: races are
	// real, retried, and never lose an update.
	clock := newFakeClock()
	mem, _ := NewMemoryStore(WithClock(clock.Now))
	l, err := New("cas", GCRA(1000, 40), casOnly{mem}, WithMaxAttempts(1000))
	must(t, err)
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
	want := fmt.Sprintf("ratelimit: name=%s algorithm=gcra keys=1 allowed=1 limited=1 errors=0 conflicts=0", h.Stats().Name)
	if got := h.Stats().String(); got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
}

var _benchSink Decision

func benchLimiter(b *testing.B, algo Algorithm) *Limiter {
	b.Helper()
	store, err := NewMemoryStore()
	if err != nil {
		b.Fatal(err)
	}
	l, err := New("bench", algo, store)
	if err != nil {
		b.Fatal(err)
	}
	return l
}

// The allowed path with a memory store, per algorithm. Rates are high enough
// that the bucket never empties, so every call takes the write path.
func BenchmarkAllowGCRA(b *testing.B) {
	b.ReportAllocs()
	l := benchLimiter(b, GCRA(1e9, 1<<30))
	ctx := context.Background()
	for b.Loop() {
		_benchSink, _ = l.Allow(ctx, "k")
	}
}

func BenchmarkAllowFixedWindow(b *testing.B) {
	b.ReportAllocs()
	l := benchLimiter(b, FixedWindow(1<<30, time.Hour))
	ctx := context.Background()
	for b.Loop() {
		_benchSink, _ = l.Allow(ctx, "k")
	}
}

func BenchmarkAllowSlidingWindow(b *testing.B) {
	b.ReportAllocs()
	l := benchLimiter(b, SlidingWindow(1<<30, time.Hour))
	ctx := context.Background()
	for b.Loop() {
		_benchSink, _ = l.Allow(ctx, "k")
	}
}

// The refused path: an exhausted key, so Step leaves the state unchanged and
// nothing is written.
func BenchmarkAllowRefused(b *testing.B) {
	b.ReportAllocs()
	l := benchLimiter(b, GCRA(1e-9, 1))
	ctx := context.Background()
	l.Allow(ctx, "k")
	for b.Loop() {
		_benchSink, _ = l.Allow(ctx, "k")
	}
}

// Distinct keys on every call: the map and LRU cost, with eviction once the
// key bound is passed.
func BenchmarkAllowManyKeys(b *testing.B) {
	b.ReportAllocs()
	l := benchLimiter(b, GCRA(1e9, 1<<30))
	ctx := context.Background()
	keys := make([]string, 4096)
	for i := range keys {
		keys[i] = fmt.Sprintf("tenant-%d", i)
	}
	i := 0
	for b.Loop() {
		_benchSink, _ = l.Allow(ctx, keys[i&4095])
		i++
	}
}

func BenchmarkAllowParallel(b *testing.B) {
	b.ReportAllocs()
	l := benchLimiter(b, GCRA(1e9, 1<<30))
	ctx := context.Background()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			l.Allow(ctx, "k")
		}
	})
}
