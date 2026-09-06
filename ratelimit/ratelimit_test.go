package ratelimit

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mgiaccone/keel/breaker"
)

// must fails the test now if err is not nil. It exists so the constructors
// this suite calls constantly don't each need a three-line
// if err != nil { t.Fatal(err) } beside them.
func must(t testing.TB, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

var _nameSeq atomic.Uint64

// uniqueName gives a limiter a name no other run in this process has used, so
// tests asserting absolute metric values are not confused by -count.
func uniqueName(t *testing.T) string {
	return fmt.Sprintf("%s#%d", t.Name(), _nameSeq.Add(1))
}

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
	must(t, err)
	l, err := New(uniqueName(t), algo, store)
	must(t, err)
	return &harness{Limiter: l, store: store, clock: clock}
}

func (h *harness) allow(t *testing.T, key string) Decision {
	t.Helper()
	d, err := h.Allow(context.Background(), key)
	must(t, err)
	return d
}

// failingStore errors on everything.
type failingStore struct{ err error }

func (f failingStore) Get(context.Context, string) (Record, time.Time, error) {
	return Record{}, time.Time{}, f.err
}
func (f failingStore) CompareAndSet(context.Context, string, uint64, State, time.Duration) (bool, error) {
	return false, f.err
}

func TestLimitedErrorIs(t *testing.T) {
	err := error(&LimitedError{Key: "acme", RetryAfter: time.Second})
	if !errors.Is(err, ErrLimited) || !strings.Contains(err.Error(), "acme") {
		t.Fatalf("err = %v", err)
	}
	// The contract package retry relies on: a refusal is retryable, after RetryAfter.
	r, ok := err.(interface {
		Retryable() bool
		RetryDelay() time.Duration
	})
	if !ok || !r.Retryable() || r.RetryDelay() != time.Second {
		t.Fatalf("retry contract: %v", err)
	}
	// The empty key, what every AdmissionGlobal refusal carries, takes the
	// friendlier un-quoted branch, not just an empty %q.
	if got, want := (&LimitedError{RetryAfter: time.Second}).Error(), "ratelimit: limit exceeded, retry after 1s"; got != want {
		t.Fatalf("empty-key Error() = %q, want %q", got, want)
	}
}

func TestAdmissionDeniesThroughBreaker(t *testing.T) {
	h := newHarness(t, GCRA(1, 2))
	b, err := breaker.New("db", breaker.WithAdmission(AdmissionGlobal(h.Limiter)))
	must(t, err)
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
	b, err := breaker.New("db", breaker.WithFailureThreshold(1), breaker.WithAdmission(AdmissionGlobal(h.Limiter)))
	must(t, err)
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
	closed, _ := breaker.New("closed", breaker.WithAdmission(AdmissionGlobal(l)))
	if _, err := closed.Do(context.Background(), func(context.Context) (int, error) { return 1, nil }); !errors.Is(err, boom) {
		t.Fatalf("fail closed: err = %v", err)
	}
	var seen error
	open, _ := breaker.New("open", breaker.WithAdmission(AdmissionGlobal(FailOpen(l, func(err error) { seen = err }))))
	if _, err := open.Do(context.Background(), func(context.Context) (int, error) { return 1, nil }); err != nil {
		t.Fatalf("fail open: err = %v", err)
	}
	if !errors.Is(seen, boom) {
		t.Fatalf("onError saw %v", seen)
	}
}

func TestAdmissionGlobalIsAdmissionWithTheEmptyKey(t *testing.T) {
	h := newHarness(t, GCRA(1, 1))
	global, keyed := AdmissionGlobal(h.Limiter), Admission(h.Limiter, "")
	ctx := context.Background()
	if err := global(ctx); err != nil {
		t.Fatalf("first call: %v", err)
	}
	// They share one record: the token global spent is the one keyed wanted.
	var lim *LimitedError
	if err := keyed(ctx); !errors.Is(err, ErrLimited) || !errors.As(err, &lim) || lim.Key != "" || lim.RetryAfter != time.Second {
		t.Fatalf("err = %v, lim = %+v", err, lim)
	}
}

func TestAdmissionKeepsKeysApart(t *testing.T) {
	h := newHarness(t, GCRA(1, 1))
	ctx := context.Background()
	a, b := Admission(h.Limiter, "tenant-a"), Admission(h.Limiter, "tenant-b")
	if err := a(ctx); err != nil {
		t.Fatalf("tenant-a: %v", err)
	}
	if err := b(ctx); err != nil {
		t.Fatalf("tenant-b has its own budget: %v", err)
	}
	var lim *LimitedError
	if err := a(ctx); !errors.As(err, &lim) || lim.Key != "tenant-a" || !strings.Contains(err.Error(), `"tenant-a"`) {
		t.Fatalf("err = %v, lim = %+v", err, lim)
	}
	if err := AdmissionGlobal(h.Limiter)(ctx); err != nil { // a third budget, not a wildcard
		t.Fatalf("global: %v", err)
	}
	if s := h.Stats(); s.Keys != 3 {
		t.Fatalf("stats = %+v, want three records", s)
	}
}
