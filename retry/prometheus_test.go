package retry

import (
	"context"
	"errors"
	"testing"
	"time"

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

func TestMetricsSeriesExistAtZeroFromStart(t *testing.T) {
	reg := prometheus.NewPedanticRegistry()
	if err := Register(reg); err != nil {
		t.Fatal(err)
	}
	h := newHarness(t, Constant(250*time.Millisecond), WithMaxAttempts(2))
	name := h.Stats().Name

	series := func(vec *prometheus.CounterVec, labels ...string) float64 {
		return testutil.ToFloat64(vec.WithLabelValues(labels...))
	}
	for _, r := range _allResults {
		if got := series(_callsCounter, name, "constant", r.String()); got != 0 {
			t.Errorf("calls{%s} at start = %v", r, got)
		}
	}
	if got := series(_attemptsCounter, name, "constant"); got != 0 {
		t.Errorf("attempts at start = %v", got)
	}
}

func TestMetricsFollowEvents(t *testing.T) {
	reg := prometheus.NewPedanticRegistry()
	if err := Register(reg); err != nil {
		t.Fatal(err)
	}
	h := newHarness(t, Constant(250*time.Millisecond), WithMaxAttempts(2))
	name := h.Stats().Name

	series := func(vec *prometheus.CounterVec, labels ...string) float64 {
		return testutil.ToFloat64(vec.WithLabelValues(labels...))
	}
	h.Do(t.Context(), failing(0, errBoom, 1))                                              // success, 1 attempt
	h.Do(t.Context(), failing(5, errBoom, 1))                                              // exhausted, 2 attempts, 1 wait
	h.Do(t.Context(), func(context.Context) (int, error) { return 0, Permanent(errBoom) }) // aborted

	want := map[string]float64{"success": 1, "exhausted": 1, "aborted": 1, "canceled": 0, "budget": 0}
	for result, n := range want {
		if got := series(_callsCounter, name, "constant", result); got != n {
			t.Errorf("calls{%s} = %v, want %v", result, got, n)
		}
	}
	if got := series(_attemptsCounter, name, "constant"); got != 4 {
		t.Errorf("attempts = %v, want 4", got)
	}
	if got := series(_waitCounter, name, "constant"); got != 0.25 {
		t.Errorf("wait_seconds = %v, want 0.25", got)
	}
}

func TestMetricsLint(t *testing.T) {
	reg := prometheus.NewPedanticRegistry()
	if err := Register(reg); err != nil {
		t.Fatal(err)
	}
	h := newHarness(t, Constant(0))
	h.Do(t.Context(), failing(0, errBoom, 1))
	problems, err := testutil.GatherAndLint(reg)
	must(t, err)
	for _, p := range problems {
		t.Errorf("lint: %s: %s", p.Metric, p.Text)
	}
}

func TestRegisterWithNamespace(t *testing.T) {
	reg := prometheus.NewPedanticRegistry()
	if err := Register(reg, WithNamespace("inventory")); err != nil {
		t.Fatal(err)
	}
	newHarness(t, Constant(0))
	if n, err := testutil.GatherAndCount(reg, "inventory_retry_attempts_total"); err != nil || n == 0 {
		t.Fatalf("inventory_retry_attempts_total series = %d (err %v)", n, err)
	}
	if n, err := testutil.GatherAndCount(reg, "go_retry_attempts_total"); err != nil || n != 0 {
		t.Fatalf("default-namespace series present under a custom namespace: %d (err %v)", n, err)
	}
}

func TestRegisterRejectsBadNamespace(t *testing.T) {
	for _, ns := range []string{"", "1abc", "my-service", "a b", "x:y"} {
		reg := prometheus.NewPedanticRegistry()
		if err := Register(reg, WithNamespace(ns)); !errors.Is(err, ErrInvalidOption) {
			t.Errorf("WithNamespace(%q): err = %v, want ErrInvalidOption", ns, err)
		}
		if n, _ := testutil.GatherAndCount(reg); n != 0 {
			t.Errorf("WithNamespace(%q): metrics registered despite error", ns)
		}
	}
}

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
