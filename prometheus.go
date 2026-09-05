package breaker

import (
	"errors"
	"fmt"
	"math"
	"regexp"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// The package's metrics. Every breaker updates them from its state goroutine
// whether or not they are registered anywhere; [Register] is what publishes
// them, under a namespace it prepends.
//
// With the default namespace "go":
//
//	go_breaker_state{dependency,state="closed|open|half-open"}      gauge, one-hot
//	go_breaker_open_until_timestamp_seconds{dependency}              gauge, unix time; 0 unless open
//	go_breaker_consecutive_trips{dependency}                         gauge
//	go_breaker_in_flight{dependency}                                 gauge
//	go_breaker_in_flight_limit{dependency}                           gauge, +Inf when unlimited
//	go_breaker_ramping{dependency}                                   gauge, 1 during a recovery ramp
//	go_breaker_calls_total{dependency,result="success|failure|canceled|rejected|shed"}  counter
//	go_breaker_trips_total{dependency}                               counter
var (
	_stateGauge = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Subsystem: _subsystem, Name: "state",
		Help: "Circuit state as a one-hot gauge: exactly one state label is 1.",
	}, []string{"dependency", "state"})
	_openUntilGauge = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Subsystem: _subsystem, Name: "open_until_timestamp_seconds",
		Help: "Unix time at which the open circuit will admit a probe; 0 unless open.",
	}, []string{"dependency"})
	_consecutiveTripsGauge = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Subsystem: _subsystem, Name: "consecutive_trips",
		Help: "Trips since the circuit last closed; drives the exponential open interval.",
	}, []string{"dependency"})
	_inFlightGauge = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Subsystem: _subsystem, Name: "in_flight",
		Help: "Admitted calls that have not yet settled.",
	}, []string{"dependency"})
	_inFlightLimitGauge = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Subsystem: _subsystem, Name: "in_flight_limit",
		Help: "Current bulkhead cap on calls in flight; +Inf when unlimited. Moves under the adaptive bulkhead.",
	}, []string{"dependency"})
	_rampingGauge = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Subsystem: _subsystem, Name: "ramping",
		Help: "1 while a recovery ramp is in progress: the circuit just closed and the in-flight cap is still climbing.",
	}, []string{"dependency"})
	_callsCounter = prometheus.NewCounterVec(prometheus.CounterOpts{
		Subsystem: _subsystem, Name: "calls_total",
		Help: "Calls that reached the breaker, by result. success+failure+canceled ran; rejected and shed did not.",
	}, []string{"dependency", "result"})
	_tripsCounter = prometheus.NewCounterVec(prometheus.CounterOpts{
		Subsystem: _subsystem, Name: "trips_total",
		Help: "Total closed/half-open to open transitions.",
	}, []string{"dependency"})

	_collectors = []prometheus.Collector{
		_stateGauge, _openUntilGauge, _consecutiveTripsGauge, _inFlightGauge, _inFlightLimitGauge, _rampingGauge, _callsCounter, _tripsCounter,
	}
)

const (
	_subsystem        = "breaker"
	_defaultNamespace = "go"
)

var _reNamespace = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*$`)

// RegisterOption configures [Register].
type RegisterOption func(*registerConfig) error

type registerConfig struct {
	namespace string
}

// WithNamespace sets the first component of every metric name, replacing the
// default "go": WithNamespace("inventory") publishes inventory_breaker_state
// and so on. Use it when several services share this package and each wants
// its breakers under its own name. The namespace must be a valid metric-name
// prefix: letters, digits and underscores, not starting with a digit.
func WithNamespace(namespace string) RegisterOption {
	return func(c *registerConfig) error {
		if !_reNamespace.MatchString(namespace) {
			return fmt.Errorf("%w: WithNamespace(%q): not a valid metric name prefix", ErrInvalidOption, namespace)
		}
		c.namespace = namespace
		return nil
	}
}

// Register publishes the package's metrics through reg, under
// <namespace>_breaker_ with namespace "go" unless [WithNamespace] says
// otherwise. Call it once at bootstrap; breakers created before or after it
// report alike. Registering twice with the same registry fails with
// prometheus.AlreadyRegisteredError.
//
// Every breaker appears under its name as the dependency label. Its series
// exist, at zero, from the moment New returns and are removed when it is
// stopped, so a torn-down breaker does not keep reporting its last state.
//
// Open → half-open is evaluated lazily, when the next call arrives, so a
// breaker with no traffic keeps reporting state="open" past its deadline. The
// timestamp gauge makes that legible:
//
//	go_breaker_state{state="open"} == 1                          # open right now
//	go_breaker_open_until_timestamp_seconds - time()             # seconds to next probe; negative = overdue, no traffic
//	rate(go_breaker_calls_total{result=~"rejected|shed"}[5m])    # load being shed, by the circuit or the bulkhead
//	go_breaker_in_flight / go_breaker_in_flight_limit            # bulkhead headroom
//	go_breaker_ramping == 1                                      # recovery in progress; a low cap is expected
//	increase(go_breaker_trips_total[10m]) > 3                    # flapping
//
// For extra const labels, pass a registerer wrapped with
// [prometheus.WrapRegistererWith].
func Register(reg prometheus.Registerer, opts ...RegisterOption) error {
	cfg := registerConfig{namespace: _defaultNamespace}
	var errs []error
	for _, opt := range opts {
		if err := opt(&cfg); err != nil {
			errs = append(errs, err)
		}
	}
	if err := errors.Join(errs...); err != nil {
		return err
	}
	reg = prometheus.WrapRegistererWithPrefix(cfg.namespace+"_", reg)
	for _, c := range _collectors {
		if err := reg.Register(c); err != nil {
			return err
		}
	}
	return nil
}

// MustRegister is [Register] for bootstrap code that treats a registration
// failure as fatal, mirroring [prometheus.MustRegister]. It panics on error.
func MustRegister(reg prometheus.Registerer, opts ...RegisterOption) {
	if err := Register(reg, opts...); err != nil {
		panic(err)
	}
}

// metricsObserver is the [Observer] New attaches to every breaker. It is only
// ever called from that breaker's state goroutine. Started resolves every
// series once, so the per-call methods are a single atomic update rather than
// a label lookup.
type metricsObserver struct {
	dep string

	state            [3]prometheus.Gauge   // indexed by State
	calls            [5]prometheus.Counter // indexed by Result
	openUntil        prometheus.Gauge
	consecutiveTrips prometheus.Gauge
	inFlight         prometheus.Gauge
	inFlightLimit    prometheus.Gauge
	ramping          prometheus.Gauge
	trips            prometheus.Counter
}

var (
	_allStates  = [...]State{Closed, Open, HalfOpen}
	_allResults = [...]Result{Success, Failure, Canceled, Rejected, Shed}
)

// Started materialises every series at its initial value, so counters exist
// at zero before the first event and rate() has a baseline.
func (o *metricsObserver) Started() {
	for _, st := range _allStates {
		o.state[st] = _stateGauge.WithLabelValues(o.dep, st.String())
	}
	for _, r := range _allResults {
		o.calls[r] = _callsCounter.WithLabelValues(o.dep, r.String())
		o.calls[r].Add(0)
	}
	o.openUntil = _openUntilGauge.WithLabelValues(o.dep)
	o.consecutiveTrips = _consecutiveTripsGauge.WithLabelValues(o.dep)
	o.inFlight = _inFlightGauge.WithLabelValues(o.dep)
	o.inFlightLimit = _inFlightLimitGauge.WithLabelValues(o.dep)
	o.ramping = _rampingGauge.WithLabelValues(o.dep)
	o.trips = _tripsCounter.WithLabelValues(o.dep)

	o.setState(Closed)
	o.openUntil.Set(0)
	o.consecutiveTrips.Set(0)
	o.inFlight.Set(0)
	o.inFlightLimit.Set(math.Inf(1)) // Load follows immediately if a cap is set
	o.ramping.Set(0)
	o.trips.Add(0)
}

func (o *metricsObserver) Call(r Result) { o.calls[r].Inc() }

func (o *metricsObserver) Transition(_, to State, consecutiveTrips int, openUntil time.Time) {
	o.setState(to)
	o.consecutiveTrips.Set(float64(consecutiveTrips))
	if to == Open {
		o.trips.Inc()
		o.openUntil.Set(float64(openUntil.UnixNano()) / 1e9)
	} else {
		o.openUntil.Set(0)
	}
}

func (o *metricsObserver) Load(inFlight, limit int, ramping bool) {
	o.inFlight.Set(float64(inFlight))
	if limit > 0 {
		o.inFlightLimit.Set(float64(limit))
	} else {
		o.inFlightLimit.Set(math.Inf(1))
	}
	var r float64
	if ramping {
		r = 1
	}
	o.ramping.Set(r)
}

// Stopped removes the breaker's series.
func (o *metricsObserver) Stopped() {
	labels := prometheus.Labels{"dependency": o.dep}
	for _, c := range _collectors {
		c.(interface{ DeletePartialMatch(prometheus.Labels) int }).DeletePartialMatch(labels)
	}
}

func (o *metricsObserver) setState(current State) {
	for _, st := range _allStates {
		var v float64
		if st == current {
			v = 1
		}
		o.state[st].Set(v)
	}
}
