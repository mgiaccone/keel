package overload

import (
	"errors"
	"fmt"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/mgiaccone/keel/internal/promutil"
)

const (
	_subsystem        = "overload"
	_defaultNamespace = "go"
)

// The package's metrics. Every limiter updates them whether or not they are
// registered anywhere; [Register] is what publishes them, under a namespace it
// prepends. With the default namespace "go":
//
//	go_overload_in_flight{name}                                       gauge
//	go_overload_capacity{name}                                        gauge
//	go_overload_requests_total{name,priority,result="admitted|shed"}  counter
//	go_overload_wait_seconds{name}                                    histogram, empty without WithMaxWait
//	go_overload_service_seconds{name}                                 histogram
//
// The pair to watch is in_flight against capacity: a limiter riding its
// capacity is at its limit, and a capacity falling away from a flat in_flight
// is the algorithm deciding the work has got slower.
var (
	_inFlightGauge = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Subsystem: _subsystem, Name: "in_flight",
		Help: "Admitted requests that have not yet released their slot.",
	}, []string{"name"})
	_capacityGauge = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Subsystem: _subsystem, Name: "capacity",
		Help: "Requests the algorithm currently allows in flight at once, across every priority.",
	}, []string{"name"})
	_requestsCounter = prometheus.NewCounterVec(prometheus.CounterOpts{
		Subsystem: _subsystem, Name: "requests_total",
		Help: "Requests that reached the limiter, by priority and whether they were admitted or shed.",
	}, []string{"name", "priority", "result"})
	_waitHistogram = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Subsystem: _subsystem, Name: "wait_seconds",
		Help:    "Time a request spent queued before it was admitted or given up on. Empty without WithMaxWait.",
		Buckets: prometheus.DefBuckets,
	}, []string{"name"})
	_serviceHistogram = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Subsystem: _subsystem, Name: "service_seconds",
		Help:    "Time an admitted request held its slot: admission to release, excluding any queue wait.",
		Buckets: prometheus.DefBuckets,
	}, []string{"name"})

	_collectors = []prometheus.Collector{
		_inFlightGauge, _capacityGauge, _requestsCounter, _waitHistogram, _serviceHistogram,
	}

	_allPriorities = [...]Priority{Sheddable, Default, Critical}
)

// RegisterOption configures [Register].
type RegisterOption func(*registerConfig) error

type registerConfig struct {
	namespace string
}

// WithNamespace sets the first component of every metric name, replacing the
// default "go", so that a service's limiters carry its own name. The namespace
// must be a valid metric-name prefix: letters, digits and underscores, not
// starting with a digit.
func WithNamespace(namespace string) RegisterOption {
	return func(c *registerConfig) error {
		if err := promutil.ValidateNamespace(namespace); err != nil {
			return fmt.Errorf("%w: %v", ErrInvalidOption, err)
		}
		c.namespace = namespace
		return nil
	}
}

// Register publishes the package's metrics through reg under
// <namespace>_overload_. Call it once at bootstrap; limiters created before or
// after it report alike. Registering twice with the same registry fails with
// prometheus.AlreadyRegisteredError.
//
//	go_overload_in_flight / go_overload_capacity                                 # headroom, 1 means saturated
//	rate(go_overload_requests_total{result="shed"}[5m])
//	  / rate(go_overload_requests_total[5m])                                     # shed share; see contrib/prometheus/alerts.yaml
//	rate(go_overload_requests_total{result="shed",priority="critical"}[5m]) > 0  # shedding work that matters
//	histogram_quantile(0.99, rate(go_overload_wait_seconds_bucket[5m]))          # what the queue is costing
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
	return promutil.Register(reg, cfg.namespace, _collectors...)
}

// MustRegister is [Register] for bootstrap code that treats a registration
// failure as fatal, mirroring [prometheus.MustRegister]. It panics on error.
func MustRegister(reg prometheus.Registerer, opts ...RegisterOption) {
	if err := Register(reg, opts...); err != nil {
		panic(err)
	}
}

// metrics is one limiter's resolved series, so the hot path is an atomic
// update rather than a label lookup.
type metrics struct {
	inFlight, capacity prometheus.Gauge
	admitted, shed     [3]prometheus.Counter // indexed by Priority
	wait, service      prometheus.Observer
}

func newMetrics(name string) *metrics {
	m := &metrics{
		inFlight: _inFlightGauge.WithLabelValues(name),
		capacity: _capacityGauge.WithLabelValues(name),
		wait:     _waitHistogram.WithLabelValues(name),
		service:  _serviceHistogram.WithLabelValues(name),
	}
	for _, p := range _allPriorities {
		m.admitted[p] = _requestsCounter.WithLabelValues(name, p.String(), "admitted")
		m.shed[p] = _requestsCounter.WithLabelValues(name, p.String(), "shed")
	}
	return m
}

// started materialises every series at its initial value, so counters exist at
// zero before the first request and rate() has a baseline.
func (m *metrics) started() {
	m.inFlight.Set(0)
	m.capacity.Set(0)
	for _, p := range _allPriorities {
		m.admitted[p].Add(0)
		m.shed[p].Add(0)
	}
}
