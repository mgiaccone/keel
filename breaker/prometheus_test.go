package breaker

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

// registry returns a fresh registry with the package metrics registered. The
// vectors themselves are package-level, so each test uses its own dependency
// name and relies on Stop (via the harness cleanup) to delete its series.
func registry(t *testing.T) *prometheus.Registry {
	t.Helper()
	reg := prometheus.NewPedanticRegistry()
	if err := Register(reg); err != nil {
		t.Fatal(err)
	}
	return reg
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
	newNamedHarness(t, "ns")
	want := `
# HELP inventory_breaker_trips_total Total closed/half-open to open transitions.
# TYPE inventory_breaker_trips_total counter
inventory_breaker_trips_total{dependency="ns"} 0
`
	if err := testutil.GatherAndCompare(reg, strings.NewReader(want), "inventory_breaker_trips_total"); err != nil {
		t.Fatal(err)
	}
	if n, _ := testutil.GatherAndCount(reg, "go_breaker_trips_total"); n != 0 {
		t.Fatalf("default-namespace series present under a custom namespace: %d", n)
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
	newNamedHarness(t, "zero")
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
	if err := testutil.GatherAndCompare(reg, strings.NewReader(want), "go_breaker_calls_total", "go_breaker_state", "go_breaker_trips_total"); err != nil {
		t.Fatal(err)
	}
}

func TestMetricsFollowEvents(t *testing.T) {
	reg := registry(t)
	h := newNamedHarness(t, "events", WithFailureThreshold(2), WithOpenInterval(10*time.Second, time.Minute))
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
`
	if err := testutil.GatherAndCompare(reg, strings.NewReader(want)); err != nil {
		t.Fatal(err)
	}

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
	if err := testutil.GatherAndCompare(reg, strings.NewReader(want), "go_breaker_state", "go_breaker_open_until_timestamp_seconds", "go_breaker_consecutive_trips"); err != nil {
		t.Fatal(err)
	}
}

func TestMetricsInFlightAndLimit(t *testing.T) {
	reg := registry(t)
	h := newNamedHarness(t, "load", WithMaxInFlight(2))
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
	if err := testutil.GatherAndCompare(reg, strings.NewReader(want), "go_breaker_in_flight", "go_breaker_in_flight_limit"); err != nil {
		t.Fatal(err)
	}
	if v := testutil.ToFloat64(_callsCounter.WithLabelValues("load", "shed")); v != 1 {
		t.Fatalf("shed = %v, want 1", v)
	}
	close(finish)
}

func TestMetricsRamping(t *testing.T) {
	reg := registry(t)
	h := newNamedHarness(t, "ramp", WithFailureThreshold(1), WithSuccessThreshold(1), WithMaxInFlight(4), WithRecoveryRamp(1, 2))
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
	if err := testutil.GatherAndCompare(reg, strings.NewReader(want), "go_breaker_in_flight_limit", "go_breaker_ramping"); err != nil {
		t.Fatal(err)
	}
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

	if n, _ := testutil.GatherAndCount(reg, "go_breaker_state"); n != 6 {
		t.Fatalf("go_breaker_state series = %d, want 6 (two breakers × three states)", n)
	}
	if v := testutil.ToFloat64(_stateGauge.WithLabelValues("many-a", "open")); v != 1 {
		t.Errorf("many-a open = %v, want 1", v)
	}
	if v := testutil.ToFloat64(_stateGauge.WithLabelValues("many-b", "closed")); v != 1 {
		t.Errorf("many-b closed = %v, want 1", v)
	}

	// Stopping one breaker removes only its series.
	a.Stop()
	if n, _ := testutil.GatherAndCount(reg, "go_breaker_state"); n != 3 {
		t.Fatalf("go_breaker_state series after Stop = %d, want 3", n)
	}
	if n, _ := testutil.GatherAndCount(reg, "go_breaker_calls_total"); n != 6 {
		t.Fatalf("go_breaker_calls_total series after Stop = %d, want 6", n)
	}
}
