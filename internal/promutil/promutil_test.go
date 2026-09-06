package promutil

import (
	"errors"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
)

func gauge(name string) prometheus.Gauge {
	return prometheus.NewGauge(prometheus.GaugeOpts{Subsystem: "sub", Name: name, Help: name})
}

func TestRegisterIsAllOrNothing(t *testing.T) {
	reg := prometheus.NewPedanticRegistry()
	// Something else already owns go_sub_b.
	if err := reg.Register(prometheus.NewCounter(prometheus.CounterOpts{Name: "go_sub_b", Help: "taken"})); err != nil {
		t.Fatal(err)
	}
	a, b, c := gauge("a"), gauge("b"), gauge("c")
	if err := Register(reg, "go", a, b, c); err == nil {
		t.Fatal("Register succeeded over a name collision")
	}
	// A plain duplicate is the documented AlreadyRegisteredError.
	if err := Register(reg, "go", b); err == nil {
		t.Fatal("registering over a name collision succeeded")
	}
	var are prometheus.AlreadyRegisteredError
	if err := Register(reg, "go", a); err != nil {
		t.Fatalf("a was left registered by the failed call: %v", err)
	}
	if err := Register(reg, "go", a); !errors.As(err, &are) {
		t.Fatalf("second registration of a = %v, want AlreadyRegisteredError", err)
	}
	if err := Register(reg, "go", c); err != nil {
		t.Fatalf("c, never reached, is registered: %v", err)
	}
}

func TestValidateNamespace(t *testing.T) {
	for _, ns := range []string{"go", "inventory_v2", "_x"} {
		if err := ValidateNamespace(ns); err != nil {
			t.Errorf("%q rejected: %v", ns, err)
		}
	}
	for _, ns := range []string{"", "1bad", "a-b", "a b"} {
		if err := ValidateNamespace(ns); err == nil {
			t.Errorf("%q accepted", ns)
		}
	}
}
