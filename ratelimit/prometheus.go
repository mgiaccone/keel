package ratelimit

import (
	"errors"
	"fmt"
	"sync"
	"weak"

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
//	go_ratelimit_keys{limiter,algorithm}                                      gauge, records the store holds, read at scrape time
var (
	_decisionsCounter = prometheus.NewCounterVec(prometheus.CounterOpts{
		Subsystem: _subsystem, Name: "decisions_total",
		Help: "Rate limiter decisions, by result. error is a decision that could not be made, e.g. Redis unreachable.",
	}, []string{"limiter", "algorithm", "result"})
	_conflictsCounter = prometheus.NewCounterVec(prometheus.CounterOpts{
		Subsystem: _subsystem, Name: "cas_conflicts_total",
		Help: "Compare-and-set retries: two writers raced on one key. Sustained conflicts mean a hot key.",
	}, []string{"limiter", "algorithm"})
	_keysDesc = prometheus.NewDesc(prometheus.BuildFQName("", _subsystem, "keys"),
		"Records the limiter's store currently holds, read at scrape time; only for stores that can report it.",
		[]string{"limiter", "algorithm"}, nil)
	_keys = &keysCollector{}

	_collectors = []prometheus.Collector{_decisionsCounter, _conflictsCounter, _keys}
)

// keysCollector is the keys gauge. It asks each live limiter's store at
// scrape time, so a decision does no work for it: a store's key count is a
// size, and a size is read when someone looks. It holds limiters weakly,
// since a limiter has no teardown and a strong reference here would keep a
// dropped one, and its store, alive for the life of the process; a collected
// limiter's series disappears with it. Two live limiters with the same name
// and algorithm report the sum, as their counters already add up in one
// series.
type keysCollector struct {
	mu       sync.Mutex
	limiters map[weak.Pointer[Limiter]]struct{}
}

func (c *keysCollector) add(l *Limiter) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.limiters == nil {
		c.limiters = make(map[weak.Pointer[Limiter]]struct{})
	}
	c.limiters[weak.Make(l)] = struct{}{}
}

// live returns the limiters still reachable, dropping the rest.
func (c *keysCollector) live() []*Limiter {
	c.mu.Lock()
	defer c.mu.Unlock()
	live := make([]*Limiter, 0, len(c.limiters))
	for wp := range c.limiters {
		if l := wp.Value(); l != nil {
			live = append(live, l)
		} else {
			delete(c.limiters, wp)
		}
	}
	return live
}

func (c *keysCollector) Describe(ch chan<- *prometheus.Desc) { ch <- _keysDesc }

func (c *keysCollector) Collect(ch chan<- prometheus.Metric) {
	type series struct{ name, algorithm string }
	totals := make(map[series]int)
	for _, l := range c.live() {
		totals[series{l.name, l.algorithm.Name()}] += l.store.(KeyCounter).Keys()
	}

	for s, n := range totals {
		if m, err := prometheus.NewConstMetric(_keysDesc, prometheus.GaugeValue, float64(n), s.name, s.algorithm); err == nil {
			ch <- m
		}
	}
}

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
}

func newMetrics(name, algorithm string) *metrics {
	return &metrics{
		allowed:   _decisionsCounter.WithLabelValues(name, algorithm, "allowed"),
		limited:   _decisionsCounter.WithLabelValues(name, algorithm, "limited"),
		errors:    _decisionsCounter.WithLabelValues(name, algorithm, "error"),
		conflicts: _conflictsCounter.WithLabelValues(name, algorithm),
	}
}

func (m *metrics) started() {
	m.allowed.Add(0)
	m.limited.Add(0)
	m.errors.Add(0)
	m.conflicts.Add(0)
}
