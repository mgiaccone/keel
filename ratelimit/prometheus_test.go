package ratelimit

import (
	"context"
	"errors"
	"runtime"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

func TestRegisterTwiceFails(t *testing.T) {
	reg := prometheus.NewPedanticRegistry()
	if err := Register(reg); err != nil {
		t.Fatal(err)
	}
	var already prometheus.AlreadyRegisteredError
	if err := Register(reg); !errors.As(err, &already) {
		t.Fatalf("second Register = %v, want AlreadyRegisteredError", err)
	}
}

func TestDecisionsAndConflictsCountersByResult(t *testing.T) {
	reg := prometheus.NewPedanticRegistry()
	if err := Register(reg); err != nil {
		t.Fatal(err)
	}
	store, _ := NewMemoryStore()
	name := uniqueName(t)
	l, err := New(name, GCRA(1, 1), store)
	must(t, err)
	l.Allow(context.Background(), "a")
	l.Allow(context.Background(), "a")
	l.Allow(context.Background(), "b")

	// The vectors are package-level and other tests' limiters live alongside,
	// so assert on this limiter's series rather than on the whole exposition.
	series := func(vec *prometheus.CounterVec, labels ...string) float64 {
		return testutil.ToFloat64(vec.WithLabelValues(labels...))
	}
	want := map[string]float64{"allowed": 2, "limited": 1, "error": 0}
	for result, n := range want {
		if got := series(_decisionsCounter, name, "gcra", result); got != n {
			t.Errorf("%s = %v, want %v", result, got, n)
		}
	}
	if got := series(_conflictsCounter, name, "gcra"); got != 0 {
		t.Errorf("conflicts = %v, want 0", got)
	}
}

func TestMetricsLint(t *testing.T) {
	reg := prometheus.NewPedanticRegistry()
	if err := Register(reg); err != nil {
		t.Fatal(err)
	}
	store, _ := NewMemoryStore()
	l, err := New(uniqueName(t), GCRA(1, 1), store)
	must(t, err)
	l.Allow(context.Background(), "a")
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

// TestRegisterWithNamespace closes a coverage gap WithNamespace's error path
// alone left open: a custom namespace must actually reach the exposed
// metric names, not just be accepted.
func TestRegisterWithNamespace(t *testing.T) {
	reg := prometheus.NewPedanticRegistry()
	if err := Register(reg, WithNamespace("inventory")); err != nil {
		t.Fatal(err)
	}
	store, _ := NewMemoryStore()
	name := uniqueName(t)
	l, err := New(name, GCRA(1, 1), store)
	must(t, err)
	l.Allow(context.Background(), "a")
	if n, err := testutil.GatherAndCount(reg, "inventory_ratelimit_decisions_total"); err != nil || n == 0 {
		t.Fatalf("inventory_ratelimit_decisions_total series = %d (err %v)", n, err)
	}
	if n, err := testutil.GatherAndCount(reg, "go_ratelimit_decisions_total"); err != nil || n != 0 {
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

// keysSeries gathers reg and returns the keys gauge for the limiter named
// name, and whether that series was exposed at all.
func keysSeries(t *testing.T, reg prometheus.Gatherer, name string) (float64, bool) {
	t.Helper()
	families, err := reg.Gather()
	must(t, err)
	for _, f := range families {
		if f.GetName() != "go_ratelimit_keys" {
			continue
		}
		for _, m := range f.GetMetric() {
			for _, l := range m.GetLabel() {
				if l.GetName() == "limiter" && l.GetValue() == name {
					return m.GetGauge().GetValue(), true
				}
			}
		}
	}
	return 0, false
}

func TestKeysGaugeIsReadAtScrapeAndFollowsTheLimiter(t *testing.T) {
	reg := prometheus.NewPedanticRegistry()
	if err := Register(reg); err != nil {
		t.Fatal(err)
	}
	name := uniqueName(t)
	// The limiter is created and used in a function of its own, so that no
	// reference to it survives on this frame once it returns.
	func() {
		store, _ := NewMemoryStore()
		l, err := New(name, GCRA(1, 1), store)
		must(t, err)
		if got, ok := keysSeries(t, reg, name); !ok || got != 0 {
			t.Fatalf("fresh limiter: keys = %v (present %v), want 0", got, ok)
		}

		for _, k := range []string{"a", "b", "c"} {
			l.Allow(context.Background(), k)
		}
		if got, _ := keysSeries(t, reg, name); got != 3 {
			t.Fatalf("keys = %v, want 3 at scrape without a decision in between", got)
		}
	}()

	runtime.GC()
	runtime.GC()

	if got, ok := keysSeries(t, reg, name); ok {
		t.Fatalf("a collected limiter still reports keys = %v", got)
	}
}
