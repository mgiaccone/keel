package fallback_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mgiaccone/keel/fallback"
)

func must(t testing.TB, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

func newStore(t *testing.T) *fallback.MemoryStore[string, int] {
	t.Helper()
	s, err := fallback.NewMemoryStore[string, int]()
	must(t, err)
	return s
}

func ptr(v int) *int { return &v }

// countingOrigin counts calls and lets a test control what each returns.
type countingOrigin struct {
	calls atomic.Int64
	fn    func(ctx context.Context, key string) (int, bool, error)
}

func (o *countingOrigin) Get(ctx context.Context, key string) (int, bool, error) {
	o.calls.Add(1)
	return o.fn(ctx, key)
}

// wantStats is the subset of Stats a Get-outcome case checks; Gets, Refreshes,
// LoadFailures, FastErrors and the two failure-observability counters are
// exercised by other tests whose shape does not fit this table.
type wantStats struct{ served, loaded, degraded, failed, aborted, misses uint64 }

func statsOf(s fallback.Stats) wantStats {
	return wantStats{s.Served, s.Loaded, s.Degraded, s.Failed, s.Aborted, s.Misses}
}

// wantStored asserts what the fast source holds for "k" after a Get,
// for the two cases where a load writes back or evicts it.
func wantStored(want int, wantFound bool) func(*testing.T, *fallback.MemoryStore[string, int]) {
	return func(t *testing.T, store *fallback.MemoryStore[string, int]) {
		t.Helper()
		v, found, _ := store.Get(t.Context(), "k")
		if found != wantFound || (wantFound && v != want) {
			t.Fatalf("store = %v, %v, want %v, %v", v, found, want, wantFound)
		}
	}
}

// TestGetDecisionTable covers every row of the decision table that resolves
// in one Get call with one shared assertion shape: the returned value, which
// Stats counter it settles, and how many times the origin was consulted.
// Rows with a fundamentally different shape — ServeAndRefresh's asynchronous
// second call, single-flight under concurrency, Invalidate racing a load, a
// panicking origin, the lease paths — are their own tests below, per
// CLAUDE.md's rule that a table earns its place only when the assertion
// logic, not just the data, is shared.
func TestGetDecisionTable(t *testing.T) {
	wantErr := errors.New("origin down")

	cases := map[string]struct {
		seed            *int // nil = absent from the fast source
		verdict         fallback.Verdict
		ctxDone         bool
		originV         int
		originFound     bool
		originErr       error
		wantOriginCalls int64
		wantV           int
		wantFound       bool
		wantErr         error
		want            wantStats
		check           func(*testing.T, *fallback.MemoryStore[string, int])
	}{
		"serve: fast value returned with no load": {
			seed: ptr(42), verdict: fallback.Serve,
			wantOriginCalls: 0, wantV: 42, wantFound: true,
			want: wantStats{served: 1},
		},
		"load or serve, load succeeds": {
			seed: ptr(1), verdict: fallback.LoadOrServe,
			originV: 9, originFound: true, wantOriginCalls: 1,
			wantV: 9, wantFound: true,
			want: wantStats{loaded: 1},
		},
		"load or serve, load fails: degrades to the stale value": {
			seed: ptr(1), verdict: fallback.LoadOrServe,
			originErr: wantErr, wantOriginCalls: 1,
			wantV: 1, wantFound: true,
			want: wantStats{degraded: 1},
		},
		"load, load succeeds": {
			seed: ptr(1), verdict: fallback.Load,
			originV: 9, originFound: true, wantOriginCalls: 1,
			wantV: 9, wantFound: true,
			want: wantStats{loaded: 1},
		},
		"load, load fails: the caller gets the error": {
			seed: ptr(1), verdict: fallback.Load,
			originErr: wantErr, wantOriginCalls: 1,
			wantErr: wantErr,
			want:    wantStats{failed: 1},
		},
		"absent from the fast source, load succeeds: fills it": {
			seed: nil, verdict: fallback.Load,
			originV: 7, originFound: true, wantOriginCalls: 1,
			wantV: 7, wantFound: true,
			want:  wantStats{loaded: 1, misses: 1},
			check: wantStored(7, true),
		},
		"absent from the fast source, load fails": {
			seed: nil, verdict: fallback.Load,
			originErr: wantErr, wantOriginCalls: 1,
			wantErr: wantErr,
			want:    wantStats{failed: 1, misses: 1},
		},
		"origin authoritatively reports the key gone: evicts it": {
			seed: ptr(1), verdict: fallback.Load,
			originV: 0, originFound: false, wantOriginCalls: 1,
			wantFound: false,
			want:      wantStats{loaded: 1, misses: 1},
			check:     wantStored(0, false),
		},
		"ctx already done: neither source consulted": {
			ctxDone: true, wantOriginCalls: 0,
			wantErr: context.Canceled,
			want:    wantStats{aborted: 1},
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			ctx := t.Context()
			if tc.ctxDone {
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
			}

			store := newStore(t)
			if tc.seed != nil {
				must(t, store.Set(t.Context(), "k", *tc.seed))
			}
			origin := &countingOrigin{fn: func(context.Context, string) (int, bool, error) {
				return tc.originV, tc.originFound, tc.originErr
			}}

			r, err := fallback.New[string, int](t.Name(), store, origin,
				fallback.WithPolicy(fallback.Policy[int](func(int, time.Time) fallback.Verdict { return tc.verdict })),
			)
			must(t, err)

			v, found, err := r.Get(ctx, "k")
			switch {
			case tc.wantErr != nil:
				if !errors.Is(err, tc.wantErr) {
					t.Fatalf("err = %v, want %v", err, tc.wantErr)
				}
			case err != nil || found != tc.wantFound || v != tc.wantV:
				t.Fatalf("Get = %v, %v, %v, want %v, %v, nil", v, found, err, tc.wantV, tc.wantFound)
			}

			if got := origin.calls.Load(); got != tc.wantOriginCalls {
				t.Fatalf("origin called %d times, want %d", got, tc.wantOriginCalls)
			}
			if got := statsOf(r.Stats()); got != tc.want {
				t.Fatalf("Stats = %+v, want %+v", got, tc.want)
			}
			if tc.check != nil {
				tc.check(t, store)
			}
		})
	}
}

// Row 2/2a: ServeAndRefresh — value served immediately, exactly one refresh
// runs. Its own test: the assertion happens after an asynchronous second
// call (the refresh), not within the triggering Get, so it does not share
// TestGetDecisionTable's shape.
func TestGetServesAndRefreshesInBackground(t *testing.T) {
	ctx := t.Context()
	store := newStore(t)
	must(t, store.Set(ctx, "k", 1))
	origin := &countingOrigin{fn: func(context.Context, string) (int, bool, error) { return 2, true, nil }}

	r, err := fallback.New[string, int]("t", store, origin,
		fallback.WithPolicy(fallback.Policy[int](func(int, time.Time) fallback.Verdict { return fallback.ServeAndRefresh })),
	)
	must(t, err)

	v, found, err := r.Get(ctx, "k")
	if err != nil || !found || v != 1 {
		t.Fatalf("Get = %v, %v, %v (want the stale value immediately)", v, found, err)
	}

	must(t, r.Close(t.Context()))
	if got := origin.calls.Load(); got != 1 {
		t.Fatalf("origin called %d times, want 1", got)
	}
	if nv, _, _ := store.Get(ctx, "k"); nv != 2 {
		t.Fatalf("store after refresh = %d, want 2", nv)
	}
	if st := r.Stats(); st.Served != 1 || st.Refreshes != 1 || st.Loaded != 0 {
		t.Fatalf("Stats = %+v", st)
	}
}

// N concurrent misses on the same key collapse to exactly one load.
func TestConcurrentMissesCoalesceToOneLoad(t *testing.T) {
	ctx := t.Context()
	store := newStore(t)
	release := make(chan struct{})
	origin := &countingOrigin{fn: func(context.Context, string) (int, bool, error) {
		<-release
		return 5, true, nil
	}}

	r, err := fallback.New[string, int]("t", store, origin)
	must(t, err)

	const n = 20
	var wg sync.WaitGroup
	results := make([]int, n)
	wg.Add(n)
	for i := range n {
		go func(i int) {
			defer wg.Done()
			v, _, err := r.Get(ctx, "k")
			if err != nil {
				t.Errorf("Get: %v", err)
			}
			results[i] = v
		}(i)
	}
	time.Sleep(20 * time.Millisecond) // let every goroutine reach the load and join it
	close(release)
	wg.Wait()

	if got := origin.calls.Load(); got != 1 {
		t.Fatalf("origin called %d times, want 1", got)
	}
	for i, v := range results {
		if v != 5 {
			t.Fatalf("results[%d] = %d, want 5", i, v)
		}
	}
}

// Invalidate during an in-flight load poisons its write-back.
func TestInvalidateDuringLoadPoisonsWriteBack(t *testing.T) {
	ctx := t.Context()
	store := newStore(t)
	proceed := make(chan struct{})
	origin := &countingOrigin{fn: func(context.Context, string) (int, bool, error) {
		<-proceed
		return 99, true, nil
	}}

	r, err := fallback.New[string, int]("t", store, origin)
	must(t, err)

	done := make(chan struct{})
	var v int
	var found bool
	go func() {
		defer close(done)
		v, found, _ = r.Get(ctx, "k")
	}()

	time.Sleep(20 * time.Millisecond) // let the load start
	must(t, r.Invalidate(ctx, "k"))
	close(proceed)
	<-done

	if !found || v != 99 {
		t.Fatalf("the waiting Get should still see the load's own result: v=%d found=%v", v, found)
	}
	if _, ffound, _ := store.Get(ctx, "k"); ffound {
		t.Fatal("the poisoned load's write-back must not resurrect the key")
	}
}

// A panicking origin is recovered as a load failure, not propagated.
func TestPanicInOriginBecomesAPanicError(t *testing.T) {
	store := newStore(t)
	origin := &countingOrigin{fn: func(context.Context, string) (int, bool, error) {
		panic("boom")
	}}

	r, err := fallback.New[string, int]("t", store, origin)
	must(t, err)

	_, _, err = r.Get(t.Context(), "k")
	var pe *fallback.PanicError
	if !errors.As(err, &pe) {
		t.Fatalf("Get err = %v, want *PanicError", err)
	}
}

// Every invalid combination is reported by New, not just the first: a
// regression here would still pass a test that only checked errors.Is.
func TestNewReportsEveryInvalidOption(t *testing.T) {
	store := newStore(t)
	origin := &countingOrigin{fn: func(context.Context, string) (int, bool, error) { return 0, false, nil }}

	_, err := fallback.New[string, int]("", store, origin, fallback.WithLoadTimeout(0))
	if !errors.Is(err, fallback.ErrInvalidOption) {
		t.Fatalf("err = %v, want wrapping ErrInvalidOption", err)
	}

	joined, ok := err.(interface{ Unwrap() []error })
	if !ok {
		t.Fatalf("err = %T, want the errors.Join-produced multi-error type", err)
	}
	if got := len(joined.Unwrap()); got != 2 {
		t.Fatalf("New joined %d errors, want 2 (empty name, and WithLoadTimeout(0))", got)
	}
}

// fakeLeaser lets a test control exactly when Acquire returns and what it
// returns, to exercise the background-refresh lease paths deterministically.
type fakeLeaser struct {
	proceed chan struct{} // Acquire blocks here until closed, if non-nil
	ok      bool
	err     error

	acquireCalls, releaseCalls atomic.Int64
}

func (l *fakeLeaser) Acquire(_ context.Context, _ string, _ time.Duration) (string, bool, error) {
	l.acquireCalls.Add(1)
	if l.proceed != nil {
		<-l.proceed
	}
	if l.err != nil {
		return "", false, l.err
	}
	return "token", l.ok, nil
}

func (l *fakeLeaser) Release(context.Context, string, string) error {
	l.releaseCalls.Add(1)
	return nil
}

// Row 2c: a background refresh whose lease is held elsewhere skips
// entirely, with nobody waiting on it — no load, no counters, no release
// (there was never a token to release).
func TestServeAndRefreshSkipsSilentlyWhenLeaseHeldElsewhere(t *testing.T) {
	ctx := t.Context()
	store := newStore(t)
	must(t, store.Set(ctx, "k", 1))
	lease := &fakeLeaser{ok: false}
	origin := &countingOrigin{fn: func(context.Context, string) (int, bool, error) {
		t.Fatal("origin must not be called: the lease was held elsewhere")
		return 0, false, nil
	}}

	r, err := fallback.New[string, int]("t", store, origin,
		fallback.WithLease[string](lease, time.Second),
		fallback.WithPolicy(fallback.Policy[int](func(int, time.Time) fallback.Verdict { return fallback.ServeAndRefresh })),
	)
	must(t, err)

	_, _, err = r.Get(ctx, "k")
	must(t, err)
	must(t, r.Close(t.Context()))

	if got := lease.acquireCalls.Load(); got != 1 {
		t.Fatalf("Acquire called %d times, want 1", got)
	}
	if got := lease.releaseCalls.Load(); got != 0 {
		t.Fatalf("Release called %d times, want 0 (no lease was held)", got)
	}
	if st := r.Stats(); st.Refreshes != 0 || st.LoadFailures != 0 {
		t.Fatalf("Stats = %+v, want no refresh counted either way", st)
	}
}

// Row 2b: a background refresh that acquires its lease but whose load fails
// counts LoadFailures, not Failed — no Get was resolved by it.
func TestServeAndRefreshCountsLoadFailureWhenLeaseAcquired(t *testing.T) {
	ctx := t.Context()
	store := newStore(t)
	must(t, store.Set(ctx, "k", 1))
	lease := &fakeLeaser{ok: true}
	wantErr := errors.New("origin down")
	origin := &countingOrigin{fn: func(context.Context, string) (int, bool, error) { return 0, false, wantErr }}

	r, err := fallback.New[string, int]("t", store, origin,
		fallback.WithLease[string](lease, time.Second),
		fallback.WithPolicy(fallback.Policy[int](func(int, time.Time) fallback.Verdict { return fallback.ServeAndRefresh })),
	)
	must(t, err)

	_, _, err = r.Get(ctx, "k")
	must(t, err)
	must(t, r.Close(t.Context()))

	if got := lease.releaseCalls.Load(); got != 1 {
		t.Fatalf("Release called %d times, want 1 (the lease was acquired)", got)
	}
	if st := r.Stats(); st.LoadFailures != 1 || st.Failed != 0 || st.Gets != 1 {
		t.Fatalf("Stats = %+v", st)
	}
}

// Regression for the bug an adversarial review found: a background refresh
// that loses its lease race must not resolve with a fabricated "not found"
// if a blocking Get has already joined its flight — that answer would be
// indistinguishable from the origin's own authoritative one. The refresh
// must fall back to a real load for that caller's sake.
func TestBlockingGetGetsARealAnswerWhenAConcurrentRefreshLosesItsLease(t *testing.T) {
	ctx := t.Context()
	store := newStore(t)
	must(t, store.Set(ctx, "k", 1))

	proceed := make(chan struct{})
	lease := &fakeLeaser{proceed: proceed, ok: false} // always loses the race
	origin := &countingOrigin{fn: func(context.Context, string) (int, bool, error) { return 42, true, nil }}

	var verdicts atomic.Int64 // first Get: ServeAndRefresh; second: LoadOrServe
	r, err := fallback.New[string, int]("t", store, origin,
		fallback.WithLease[string](lease, time.Second),
		fallback.WithPolicy(fallback.Policy[int](func(int, time.Time) fallback.Verdict {
			if verdicts.Add(1) == 1 {
				return fallback.ServeAndRefresh
			}
			return fallback.LoadOrServe
		})),
	)
	must(t, err)

	// Get A: served from the stale value, starts a background refresh whose
	// lease.Acquire is now blocked on proceed.
	_, _, err = r.Get(ctx, "k")
	must(t, err)

	// Get B joins the same in-flight call before the lease decision resolves.
	done := make(chan struct{})
	var v int
	var found bool
	var getErr error
	go func() {
		defer close(done)
		v, found, getErr = r.Get(ctx, "k")
	}()

	time.Sleep(20 * time.Millisecond) // let Get B reach the join before releasing the race
	close(proceed)
	<-done

	if getErr != nil || !found || v != 42 {
		t.Fatalf("Get B = %v, %v, %v, want the origin's real answer (42, true, nil), not a fabricated miss", v, found, getErr)
	}
	if got := origin.calls.Load(); got != 1 {
		t.Fatalf("origin called %d times, want 1", got)
	}
}

func BenchmarkGetServed(b *testing.B) {
	ctx := b.Context()
	store := mustStore(b)
	must(b, store.Set(ctx, "k", 1))
	origin := &countingOrigin{fn: func(context.Context, string) (int, bool, error) { return 0, false, nil }}
	r, err := fallback.New[string, int]("bench", store, origin)
	must(b, err)

	b.ReportAllocs()
	for b.Loop() {
		if _, _, err := r.Get(ctx, "k"); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkGetManyKeys(b *testing.B) {
	ctx := b.Context()
	store, err := fallback.NewMemoryStore[string, int](fallback.WithMaxKeys(4096))
	must(b, err)
	keys := make([]string, 4096)
	for i := range keys {
		keys[i] = fmt.Sprintf("k%d", i)
		must(b, store.Set(ctx, keys[i], i))
	}
	origin := &countingOrigin{fn: func(context.Context, string) (int, bool, error) { return 0, false, nil }}
	r, err := fallback.New[string, int]("bench", store, origin)
	must(b, err)

	b.ReportAllocs()
	i := 0
	for b.Loop() {
		if _, _, err := r.Get(ctx, keys[i&4095]); err != nil {
			b.Fatal(err)
		}
		i++
	}
}

func BenchmarkGetParallel(b *testing.B) {
	ctx := b.Context()
	store := mustStore(b)
	must(b, store.Set(ctx, "k", 1))
	origin := &countingOrigin{fn: func(context.Context, string) (int, bool, error) { return 0, false, nil }}
	r, err := fallback.New[string, int]("bench", store, origin)
	must(b, err)

	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if _, _, err := r.Get(ctx, "k"); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func mustStore(b *testing.B) *fallback.MemoryStore[string, int] {
	b.Helper()
	s, err := fallback.NewMemoryStore[string, int]()
	must(b, err)
	return s
}
