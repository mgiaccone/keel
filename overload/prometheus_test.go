package overload

import (
	"errors"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

func TestRegisterTwiceFails(t *testing.T) {
	reg := prometheus.NewPedanticRegistry()
	must(t, Register(reg))
	var already prometheus.AlreadyRegisteredError
	if err := Register(reg); !errors.As(err, &already) {
		t.Fatalf("second Register = %v, want AlreadyRegisteredError", err)
	}
}

func TestRequestsCounterByPriorityAndResult(t *testing.T) {
	reg := prometheus.NewPedanticRegistry()
	must(t, Register(reg))

	h := newHarness(t, fixed{capacity: 2}, 0.6, 0.85)
	first, second := h.acquire(t, Critical), h.acquire(t, Critical)
	h.shed(t, Critical)
	h.shed(t, Sheddable)
	first(nil)
	second(nil)

	series := func(p Priority, result string) float64 {
		return testutil.ToFloat64(_requestsCounter.WithLabelValues(h.name, p.String(), result))
	}
	for _, want := range []struct {
		p      Priority
		result string
		n      float64
	}{
		{Critical, "admitted", 2},
		{Critical, "shed", 1},
		{Sheddable, "shed", 1},
		{Sheddable, "admitted", 0},
		{Default, "shed", 0},
	} {
		if got := series(want.p, want.result); got != want.n {
			t.Errorf("requests_total{priority=%s,result=%s} = %v, want %v", want.p, want.result, got, want.n)
		}
	}
}

func TestCapacityAndInFlightGaugesFollowTheLimiter(t *testing.T) {
	reg := prometheus.NewPedanticRegistry()
	must(t, Register(reg))

	h := newHarness(t, AIMD(1, 4, time.Millisecond, time.Millisecond), 0.6, 0.85)
	if got := testutil.ToFloat64(_capacityGauge.WithLabelValues(h.name)); got != 4 {
		t.Errorf("capacity = %v, want the opening capacity published before the first request", got)
	}

	release := h.acquire(t, Critical)
	// The in-flight gauge is only written when a slot is taken or given back,
	// which is what makes the hot path one atomic store rather than a lookup.
	if got := testutil.ToFloat64(_inFlightGauge.WithLabelValues(h.name)); got != 1 {
		t.Errorf("in_flight = %v, want 1", got)
	}

	h.clock.Add(time.Second)
	release(errors.New("boom"))
	release2 := h.acquire(t, Critical)
	h.clock.Add(time.Second)
	release2(errors.New("boom"))
	if got := testutil.ToFloat64(_capacityGauge.WithLabelValues(h.name)); got != 2 {
		t.Errorf("capacity = %v, want the halving published", got)
	}
	if got := testutil.ToFloat64(_inFlightGauge.WithLabelValues(h.name)); got != 0 {
		t.Errorf("in_flight = %v, want 0", got)
	}
}

// observations is how many samples the named histogram holds for one limiter.
func observations(t testing.TB, reg *prometheus.Registry, metric, limiter string) uint64 {
	t.Helper()
	families, err := reg.Gather()
	must(t, err)
	for _, f := range families {
		if f.GetName() != metric {
			continue
		}
		for _, m := range f.GetMetric() {
			for _, l := range m.GetLabel() {
				if l.GetName() == "name" && l.GetValue() == limiter {
					return m.GetHistogram().GetSampleCount()
				}
			}
		}
	}
	return 0
}

func TestWaitHistogramStaysEmptyWithoutTheQueue(t *testing.T) {
	reg := prometheus.NewPedanticRegistry()
	must(t, Register(reg))

	h := newHarness(t, fixed{capacity: 1}, 0.6, 0.85)
	release := h.acquire(t, Critical)
	h.shed(t, Critical)
	release(nil)

	// The service histogram has the one completed request; the wait histogram
	// has nothing, because nothing queued.
	if got := observations(t, reg, "go_overload_service_seconds", h.name); got != 1 {
		t.Errorf("service_seconds samples = %d, want 1", got)
	}
	if got := observations(t, reg, "go_overload_wait_seconds", h.name); got != 0 {
		t.Errorf("wait_seconds samples = %d, want 0 without WithMaxWait", got)
	}

	queued := newHarness(t, fixed{capacity: 1}, 0.6, 0.85, WithMaxWait(5*time.Millisecond, time.Hour, time.Hour))
	held := queued.acquire(t, Critical)
	queued.shed(t, Critical)
	held(nil)
	if got := observations(t, reg, "go_overload_wait_seconds", queued.name); got != 1 {
		t.Errorf("wait_seconds samples = %d, want the one wait that happened", got)
	}
}

func TestMetricsLint(t *testing.T) {
	reg := prometheus.NewPedanticRegistry()
	must(t, Register(reg))

	h := newHarness(t, fixed{capacity: 1}, 0.6, 0.85, WithMaxWait(time.Millisecond, time.Millisecond, time.Second))
	release := h.acquire(t, Critical)
	h.shed(t, Default)
	release(nil)

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

func TestRegisterWithNamespace(t *testing.T) {
	reg := prometheus.NewPedanticRegistry()
	must(t, Register(reg, WithNamespace("inventory")))
	h := newHarness(t, fixed{capacity: 1}, 0.6, 0.85)
	h.acquire(t, Critical)(nil)

	if n, err := testutil.GatherAndCount(reg, "inventory_overload_requests_total"); err != nil || n == 0 {
		t.Fatalf("inventory_overload_requests_total series = %d (err %v)", n, err)
	}
	if n, err := testutil.GatherAndCount(reg, "go_overload_requests_total"); err != nil || n != 0 {
		t.Fatalf("default-namespace series present under a custom namespace: %d (err %v)", n, err)
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
