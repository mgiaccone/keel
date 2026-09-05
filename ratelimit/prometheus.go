package ratelimit

import (
	"errors"
	"fmt"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/mgiaccone/keel/internal/promutil"
)

const (
	_subsystem        = "ratelimit"
	_defaultNamespace = "go"
)

// The package's metrics. Every limiter updates them; [Register] publishes
// them under a namespace it prepends. With the default namespace "go":
//
//	go_ratelimit_decisions_total{limiter,algorithm,result="allowed|limited|error"}  counter
//	go_ratelimit_cas_conflicts_total{limiter,algorithm}                       counter, compare-and-set retries
//	go_ratelimit_keys{limiter,algorithm}                                      gauge, records the store holds, if it can say
var (
	_decisionsCounter = prometheus.NewCounterVec(prometheus.CounterOpts{
		Subsystem: _subsystem, Name: "decisions_total",
		Help: "Rate limiter decisions, by result. error is a decision that could not be made, e.g. Redis unreachable.",
	}, []string{"limiter", "algorithm", "result"})
	_conflictsCounter = prometheus.NewCounterVec(prometheus.CounterOpts{
		Subsystem: _subsystem, Name: "cas_conflicts_total",
		Help: "Compare-and-set retries: two writers raced on one key. Sustained conflicts mean a hot key.",
	}, []string{"limiter", "algorithm"})
	_keysGauge = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Subsystem: _subsystem, Name: "keys",
		Help: "Records the limiter's store currently holds, when the store can report it.",
	}, []string{"limiter", "algorithm"})

	_collectors = []prometheus.Collector{_decisionsCounter, _conflictsCounter, _keysGauge}
)

// RegisterOption configures [Register].
type RegisterOption func(*registerConfig) error

type registerConfig struct {
	namespace string
}

// WithNamespace sets the first component of every metric name, replacing the
// default "go", so that a service's limiters carry its own name.
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
// <namespace>_ratelimit_. Call it once at bootstrap.
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

// MustRegister is [Register] that panics on error, mirroring
// [prometheus.MustRegister], for bootstrap code.
func MustRegister(reg prometheus.Registerer, opts ...RegisterOption) {
	if err := Register(reg, opts...); err != nil {
		panic(err)
	}
}

// metrics is one limiter's resolved series.
type metrics struct {
	allowed, limited, errors, conflicts prometheus.Counter
	keys                                prometheus.Gauge
}

func newMetrics(name, algorithm string) *metrics {
	return &metrics{
		allowed:   _decisionsCounter.WithLabelValues(name, algorithm, "allowed"),
		limited:   _decisionsCounter.WithLabelValues(name, algorithm, "limited"),
		errors:    _decisionsCounter.WithLabelValues(name, algorithm, "error"),
		conflicts: _conflictsCounter.WithLabelValues(name, algorithm),
		keys:      _keysGauge.WithLabelValues(name, algorithm),
	}
}

func (m *metrics) started() {
	m.allowed.Add(0)
	m.limited.Add(0)
	m.errors.Add(0)
	m.conflicts.Add(0)
	m.keys.Set(0)
}
