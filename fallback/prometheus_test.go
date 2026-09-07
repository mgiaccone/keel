package fallback

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

// must fails the test now if err is not nil.
func must(t testing.TB, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

var _nameSeq atomic.Uint64

// uniqueName gives a reader a name no other run in this process has used, so
// tests asserting absolute metric values are not confused by -count.
func uniqueName(t *testing.T) string {
	return fmt.Sprintf("%s#%d", t.Name(), _nameSeq.Add(1))
}

func newTestReader(t *testing.T, name string, seed map[string]int, loadFn SourceFunc[string, int]) *Reader[string, int] {
	t.Helper()
	store, err := NewMemoryStore[string, int]()
	must(t, err)
	for k, v := range seed {
		must(t, store.Set(t.Context(), k, v))
	}
	r, err := New(name, store, loadFn)
	must(t, err)
	return r
}

func TestRegisterTwiceFails(t *testing.T) {
	reg := prometheus.NewPedanticRegistry()
	must(t, Register(reg))
	var already prometheus.AlreadyRegisteredError
	if err := Register(reg); !errors.As(err, &already) {
		t.Fatalf("second Register = %v, want AlreadyRegisteredError", err)
	}
}

func TestGetsCounterByOutcome(t *testing.T) {
	reg := prometheus.NewPedanticRegistry()
	must(t, Register(reg))

	name := uniqueName(t)
	r := newTestReader(t, name, map[string]int{"hit": 1},
		func(context.Context, string) (int, bool, error) { return 2, true, nil })

	r.Get(t.Context(), "hit")  // Served: present in the fast source, default policy
	r.Get(t.Context(), "miss") // Loaded: absent from the fast source

	series := func(outcome string) float64 { return testutil.ToFloat64(_getsCounter.WithLabelValues(name, outcome)) }
	want := map[string]float64{"served": 1, "loaded": 1, "degraded": 0, "failed": 0, "aborted": 0}
	for outcome, n := range want {
		if got := series(outcome); got != n {
			t.Errorf("%s = %v, want %v", outcome, got, n)
		}
	}
}

func TestMissesFastErrorsAndLoadFailuresCounters(t *testing.T) {
	reg := prometheus.NewPedanticRegistry()
	must(t, Register(reg))

	name := uniqueName(t)
	wantErr := errors.New("origin down")
	r := newTestReader(t, name, nil, func(context.Context, string) (int, bool, error) { return 0, false, wantErr })

	r.Get(t.Context(), "k") // absent from fast, load fails: Miss + Failed

	if got := testutil.ToFloat64(_missesCounter.WithLabelValues(name)); got != 1 {
		t.Errorf("misses = %v, want 1", got)
	}
	if got := testutil.ToFloat64(_getsCounter.WithLabelValues(name, "failed")); got != 1 {
		t.Errorf("gets{outcome=failed} = %v, want 1", got)
	}
	if got := testutil.ToFloat64(_fastErrorsCounter.WithLabelValues(name)); got != 0 {
		t.Errorf("fast_errors = %v, want 0 (the fast source reported absent, not an error)", got)
	}
}

func TestMetricsLint(t *testing.T) {
	reg := prometheus.NewPedanticRegistry()
	must(t, Register(reg))
	r := newTestReader(t, uniqueName(t), nil, func(context.Context, string) (int, bool, error) { return 1, true, nil })
	r.Get(t.Context(), "k")

	problems, err := testutil.GatherAndLint(reg)
	must(t, err)
	for _, p := range problems {
		t.Errorf("lint: %s: %s", p.Metric, p.Text)
	}
}

func TestRegisterRejectsBadNamespace(t *testing.T) {
	for _, ns := range []string{"", "1bad", "my-service", "a b", "x:y"} {
		reg := prometheus.NewPedanticRegistry()
		if err := Register(reg, WithNamespace(ns)); !errors.Is(err, ErrInvalidOption) {
			t.Errorf("WithNamespace(%q): err = %v, want ErrInvalidOption", ns, err)
		}
		if n, _ := testutil.GatherAndCount(reg); n != 0 {
			t.Errorf("WithNamespace(%q): metrics registered despite error", ns)
		}
	}
}

// TestRegisterWithNamespace closes the coverage gap WithNamespace's error
// path alone leaves open: a custom namespace must actually reach the
// exposed metric names, not just be accepted.
func TestRegisterWithNamespace(t *testing.T) {
	reg := prometheus.NewPedanticRegistry()
	must(t, Register(reg, WithNamespace("inventory")))
	r := newTestReader(t, uniqueName(t), nil, func(context.Context, string) (int, bool, error) { return 1, true, nil })
	r.Get(t.Context(), "k")

	if n, err := testutil.GatherAndCount(reg, "inventory_fallback_gets_total"); err != nil || n == 0 {
		t.Fatalf("inventory_fallback_gets_total series = %d (err %v)", n, err)
	}
	if n, err := testutil.GatherAndCount(reg, "go_fallback_gets_total"); err != nil || n != 0 {
		t.Fatalf("default-namespace series present under a custom namespace: %d (err %v)", n, err)
	}
}

// TestMustRegisterPanicsOnDuplicate closes the other coverage gap: nothing
// exercised MustRegister at all before this.
func TestMustRegisterPanicsOnDuplicate(t *testing.T) {
	reg := prometheus.NewPedanticRegistry()
	MustRegister(reg)
	defer func() {
		if recover() == nil {
			t.Error("MustRegister did not panic on a duplicate registration")
		}
	}()
	MustRegister(reg)
}
