package breaker

import (
	"bytes"
	"context"
	"errors"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
)

// registry returns a fresh registry with the package metrics registered.
func registry(t *testing.T) *prometheus.Registry {
	t.Helper()
	reg := prometheus.NewPedanticRegistry()
	if err := Register(reg); err != nil {
		t.Fatal(err)
	}
	return reg
}

// compareSeries checks the exposition of one dependency's series against want.
// The vectors are package-level and other tests' breakers, and breakers not
// yet collected, live alongside, so the gathered families are filtered to the
// dependency before comparing.
func compareSeries(t *testing.T, reg prometheus.Gatherer, dependency, want string, names ...string) {
	t.Helper()
	families, err := reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	var kept []*dto.MetricFamily
	for _, f := range families {
		if len(names) > 0 && !slices.Contains(names, f.GetName()) {
			continue
		}
		var metrics []*dto.Metric
		for _, m := range f.GetMetric() {
			for _, l := range m.GetLabel() {
				if l.GetName() == "dependency" && l.GetValue() == dependency {
					metrics = append(metrics, m)
				}
			}
		}
		if len(metrics) > 0 {
			f.Metric = metrics
			kept = append(kept, f)
		}
	}
	var got bytes.Buffer
	enc := expfmt.NewEncoder(&got, expfmt.NewFormat(expfmt.TypeTextPlain))
	for _, f := range kept {
		if err := enc.Encode(f); err != nil {
			t.Fatal(err)
		}
	}
	if strings.TrimSpace(got.String()) != strings.TrimSpace(want) {
		t.Fatalf("series for %q differ:\n got:\n%s\nwant:\n%s", dependency, got.String(), want)
	}
}

// countSeries counts one dependency's series of a metric.
func countSeries(t *testing.T, reg prometheus.Gatherer, name, dependency string) int {
	t.Helper()
	families, err := reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	n := 0
	for _, f := range families {
		if f.GetName() != name {
			continue
		}
		for _, m := range f.GetMetric() {
			for _, l := range m.GetLabel() {
				if l.GetName() == "dependency" && l.GetValue() == dependency {
					n++
				}
			}
		}
	}
	return n
}

func TestRegisterTwiceFails(t *testing.T) {
	reg := registry(t)
	var already prometheus.AlreadyRegisteredError
	if err := Register(reg); !errors.As(err, &already) {
		t.Fatalf("second Register = %v, want AlreadyRegisteredError", err)
	}
	func() {
		defer func() {
			if recover() == nil {
				t.Error("MustRegister did not panic on a duplicate registration")
			}
		}()
		MustRegister(reg)
	}()
}

func TestRegisterWithNamespace(t *testing.T) {
	reg := prometheus.NewPedanticRegistry()
	if err := Register(reg, WithNamespace("inventory")); err != nil {
		t.Fatal(err)
	}
	const dep = "ns"
	newNamedHarness(t, dep)
	want := `
# HELP inventory_breaker_trips_total Total closed/half-open to open transitions.
# TYPE inventory_breaker_trips_total counter
inventory_breaker_trips_total{dependency="ns"} 0
`
	compareSeries(t, reg, dep, want, "inventory_breaker_trips_total")
	if n, err := testutil.GatherAndCount(reg, "go_breaker_trips_total"); err != nil || n != 0 {
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

func TestMetricsLint(t *testing.T) {
	reg := registry(t)
	newHarness(t)
	problems, err := testutil.GatherAndLint(reg)
	if err != nil {
		t.Fatal(err)
	}
	for _, p := range problems {
		t.Errorf("lint: %s: %s", p.Metric, p.Text)
	}
}

func TestMetricsSeriesExistAtZeroFromStart(t *testing.T) {
	reg := registry(t)
	const dep = "zero"
	newNamedHarness(t, dep)
	want := `
# HELP go_breaker_calls_total Calls that reached the breaker, by result. success+failure+canceled ran; rejected, shed and denied did not.
# TYPE go_breaker_calls_total counter
go_breaker_calls_total{dependency="zero",result="canceled"} 0
go_breaker_calls_total{dependency="zero",result="denied"} 0
go_breaker_calls_total{dependency="zero",result="failure"} 0
go_breaker_calls_total{dependency="zero",result="rejected"} 0
go_breaker_calls_total{dependency="zero",result="shed"} 0
go_breaker_calls_total{dependency="zero",result="success"} 0
# HELP go_breaker_state Circuit state as a one-hot gauge: exactly one state label is 1.
# TYPE go_breaker_state gauge
go_breaker_state{dependency="zero",state="closed"} 1
go_breaker_state{dependency="zero",state="half-open"} 0
go_breaker_state{dependency="zero",state="open"} 0
# HELP go_breaker_trips_total Total closed/half-open to open transitions.
# TYPE go_breaker_trips_total counter
go_breaker_trips_total{dependency="zero"} 0
`
	compareSeries(t, reg, dep, want, "go_breaker_calls_total", "go_breaker_state", "go_breaker_trips_total")
}

func TestMetricsFollowEvents(t *testing.T) {
	reg := registry(t)
	const dep = "events"
	h := newNamedHarness(t, dep, WithFailureThreshold(2), WithOpenInterval(10*time.Second, time.Minute))
	h.ok(t)
	h.Do(t.Context(), func(context.Context) (int, error) { return 0, context.Canceled })
	h.fail(t)
	h.fail(t) // trips at clock+10s
	h.ok(t)   // rejected
	openUntil := strconv.FormatFloat(float64(h.clock.Now().Add(10*time.Second).UnixNano())/1e9, 'g', -1, 64)

	want := `
# HELP go_breaker_calls_total Calls that reached the breaker, by result. success+failure+canceled ran; rejected, shed and denied did not.
# TYPE go_breaker_calls_total counter
go_breaker_calls_total{dependency="events",result="canceled"} 1
go_breaker_calls_total{dependency="events",result="denied"} 0
go_breaker_calls_total{dependency="events",result="failure"} 2
go_breaker_calls_total{dependency="events",result="rejected"} 1
go_breaker_calls_total{dependency="events",result="shed"} 0
go_breaker_calls_total{dependency="events",result="success"} 1
# HELP go_breaker_consecutive_trips Trips since the circuit last closed; drives the exponential open interval.
# TYPE go_breaker_consecutive_trips gauge
go_breaker_consecutive_trips{dependency="events"} 1
# HELP go_breaker_error_rate Share of calls settled while closed that failed, over the trailing WithErrorRate window; 0 when the rule is off or the window is empty.
# TYPE go_breaker_error_rate gauge
go_breaker_error_rate{dependency="events"} 0
# HELP go_breaker_in_flight Admitted calls that have not yet settled.
# TYPE go_breaker_in_flight gauge
go_breaker_in_flight{dependency="events"} 0
# HELP go_breaker_in_flight_limit Current bulkhead cap on calls in flight; +Inf when unlimited. Moves under the adaptive bulkhead.
# TYPE go_breaker_in_flight_limit gauge
go_breaker_in_flight_limit{dependency="events"} +Inf
# HELP go_breaker_open_until_timestamp_seconds Unix time at which the open circuit will admit a probe; 0 unless open.
# TYPE go_breaker_open_until_timestamp_seconds gauge
go_breaker_open_until_timestamp_seconds{dependency="events"} ` + openUntil + `
# HELP go_breaker_ramping 1 while a recovery ramp is in progress: the circuit just closed and the in-flight cap is still climbing.
# TYPE go_breaker_ramping gauge
go_breaker_ramping{dependency="events"} 0
# HELP go_breaker_state Circuit state as a one-hot gauge: exactly one state label is 1.
# TYPE go_breaker_state gauge
go_breaker_state{dependency="events",state="closed"} 0
go_breaker_state{dependency="events",state="half-open"} 0
go_breaker_state{dependency="events",state="open"} 1
# HELP go_breaker_trips_total Total closed/half-open to open transitions.
# TYPE go_breaker_trips_total counter
go_breaker_trips_total{dependency="events"} 1
# HELP go_breaker_window_calls Calls settled while closed in the trailing WithErrorRate window; 0 when the rule is off.
# TYPE go_breaker_window_calls gauge
go_breaker_window_calls{dependency="events"} 0
`
	compareSeries(t, reg, dep, want)

	// Recovery: half-open on the next call, closed after two probe successes.
	h.clock.Add(10 * time.Second)
	h.ok(t)
	h.ok(t)
	want = `
# HELP go_breaker_consecutive_trips Trips since the circuit last closed; drives the exponential open interval.
# TYPE go_breaker_consecutive_trips gauge
go_breaker_consecutive_trips{dependency="events"} 0
# HELP go_breaker_open_until_timestamp_seconds Unix time at which the open circuit will admit a probe; 0 unless open.
# TYPE go_breaker_open_until_timestamp_seconds gauge
go_breaker_open_until_timestamp_seconds{dependency="events"} 0
# HELP go_breaker_state Circuit state as a one-hot gauge: exactly one state label is 1.
# TYPE go_breaker_state gauge
go_breaker_state{dependency="events",state="closed"} 1
go_breaker_state{dependency="events",state="half-open"} 0
go_breaker_state{dependency="events",state="open"} 0
`
	compareSeries(t, reg, dep, want, "go_breaker_state", "go_breaker_open_until_timestamp_seconds", "go_breaker_consecutive_trips")
}

func TestMetricsInFlightAndLimit(t *testing.T) {
	reg := registry(t)
	const dep = "load"
	h := newNamedHarness(t, dep, WithMaxInFlight(2))
	entered, finish := make(chan struct{}, 2), make(chan struct{})
	for range 2 {
		go h.Do(context.Background(), func(context.Context) (int, error) {
			entered <- struct{}{}
			<-finish
			return 1, nil
		})
	}
	recv(t, entered)
	recv(t, entered)
	h.ok(t) // shed
	want := `
# HELP go_breaker_in_flight Admitted calls that have not yet settled.
# TYPE go_breaker_in_flight gauge
go_breaker_in_flight{dependency="load"} 2
# HELP go_breaker_in_flight_limit Current bulkhead cap on calls in flight; +Inf when unlimited. Moves under the adaptive bulkhead.
# TYPE go_breaker_in_flight_limit gauge
go_breaker_in_flight_limit{dependency="load"} 2
`
	compareSeries(t, reg, dep, want, "go_breaker_in_flight", "go_breaker_in_flight_limit")
	if v := testutil.ToFloat64(_callsCounter.WithLabelValues("load", "shed")); v != 1 {
		t.Fatalf("shed = %v, want 1", v)
	}
	close(finish)
}

func TestMetricsRamping(t *testing.T) {
	reg := registry(t)
	const dep = "ramp"
	h := newNamedHarness(t, dep, WithFailureThreshold(1), WithSuccessThreshold(1), WithMaxInFlight(4), WithRecoveryRamp(1, 2))
	h.trip(t)
	h.clock.Add(time.Second)
	h.ok(t) // close: ramping at 1
	want := `
# HELP go_breaker_in_flight_limit Current bulkhead cap on calls in flight; +Inf when unlimited. Moves under the adaptive bulkhead.
# TYPE go_breaker_in_flight_limit gauge
go_breaker_in_flight_limit{dependency="ramp"} 1
# HELP go_breaker_ramping 1 while a recovery ramp is in progress: the circuit just closed and the in-flight cap is still climbing.
# TYPE go_breaker_ramping gauge
go_breaker_ramping{dependency="ramp"} 1
`
	compareSeries(t, reg, dep, want, "go_breaker_in_flight_limit", "go_breaker_ramping")
	h.ok(t) // 1 -> 2 == end
	if v := testutil.ToFloat64(_rampingGauge.WithLabelValues("ramp")); v != 0 {
		t.Fatalf("ramping after end = %v", v)
	}
	if v := testutil.ToFloat64(_inFlightLimitGauge.WithLabelValues("ramp")); v != 4 {
		t.Fatalf("limit after ramp = %v, want 4", v)
	}
}

func TestManyBreakersOneRegistration(t *testing.T) {
	reg := registry(t)
	a := newNamedHarness(t, "many-a", WithFailureThreshold(1))
	b := newNamedHarness(t, "many-b")
	a.trip(t)
	b.ok(t)

	for _, dep := range []string{"many-a", "many-b"} {
		if n := countSeries(t, reg, "go_breaker_state", dep); n != 3 {
			t.Fatalf("%s: go_breaker_state series = %d, want 3", dep, n)
		}
	}
	if v := testutil.ToFloat64(_stateGauge.WithLabelValues("many-a", "open")); v != 1 {
		t.Errorf("many-a open = %v, want 1", v)
	}
	if v := testutil.ToFloat64(_stateGauge.WithLabelValues("many-b", "closed")); v != 1 {
		t.Errorf("many-b closed = %v, want 1", v)
	}

	// Stopping one breaker removes only its series, immediately.
	a.Stop()
	if n := countSeries(t, reg, "go_breaker_state", "many-a"); n != 0 {
		t.Fatalf("many-a series after Stop = %d, want 0", n)
	}
	if n := countSeries(t, reg, "go_breaker_calls_total", "many-b"); n != 6 {
		t.Fatalf("many-b go_breaker_calls_total series = %d, want 6", n)
	}
}

func TestMetricsErrorRate(t *testing.T) {
	reg := registry(t)
	const dep = "rate"
	h := newNamedHarness(t, dep, WithFailureThreshold(100), WithErrorRate(0.5, 100*time.Millisecond, 100))
	h.fail(t)
	h.fail(t)
	h.ok(t)
	h.ok(t)
	want := `
# HELP go_breaker_error_rate Share of calls settled while closed that failed, over the trailing WithErrorRate window; 0 when the rule is off or the window is empty.
# TYPE go_breaker_error_rate gauge
go_breaker_error_rate{dependency="rate"} 0.5
# HELP go_breaker_window_calls Calls settled while closed in the trailing WithErrorRate window; 0 when the rule is off.
# TYPE go_breaker_window_calls gauge
go_breaker_window_calls{dependency="rate"} 4
`
	compareSeries(t, reg, dep, want, "go_breaker_error_rate", "go_breaker_window_calls")

	// An inspection that rolls outcomes out of the window updates the gauges.
	h.clock.Add(100 * time.Millisecond)
	h.Stats()
	want = `
# HELP go_breaker_error_rate Share of calls settled while closed that failed, over the trailing WithErrorRate window; 0 when the rule is off or the window is empty.
# TYPE go_breaker_error_rate gauge
go_breaker_error_rate{dependency="rate"} 0
# HELP go_breaker_window_calls Calls settled while closed in the trailing WithErrorRate window; 0 when the rule is off.
# TYPE go_breaker_window_calls gauge
go_breaker_window_calls{dependency="rate"} 0
`
	compareSeries(t, reg, dep, want, "go_breaker_error_rate", "go_breaker_window_calls")

	h.Stop()
	if n := countSeries(t, reg, "go_breaker_error_rate", dep) + countSeries(t, reg, "go_breaker_window_calls", dep); n != 0 {
		t.Fatalf("%d window series survive Stop", n)
	}
}
