package fallback

import (
	"errors"
	"fmt"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/mgiaccone/keel/internal/promutil"
)

const (
	_subsystem        = "fallback"
	_defaultNamespace = "go"
)

// The package's metrics. Every reader updates them; [Register] publishes
// them under a namespace it prepends. With the default namespace "go":
//
//	go_fallback_gets_total{name,outcome="served|loaded|degraded|failed|aborted"}  counter
//	go_fallback_misses_total{name}                                               counter
//	go_fallback_refreshes_total{name}                                            counter
//	go_fallback_load_failures_total{name}                                        counter, background refreshes only
//	go_fallback_fast_errors_total{name}                                          counter
//	go_fallback_write_back_failures_total{name}                                  counter
//	go_fallback_lease_failures_total{name}                                       counter
//
// There is no entries/keys gauge: unlike [ratelimit.Store]'s optional
// [ratelimit.KeyCounter], [Store] has no equivalent capability to sniff — a
// capability is configured, not sniffed, in this package — and no option
// currently asks a caller to state one, so the fast source's size is not
// published here.
var (
	_getsCounter = prometheus.NewCounterVec(prometheus.CounterOpts{
		Subsystem: _subsystem, Name: "gets_total",
		Help: "Reader.Get calls, by outcome.",
	}, []string{"name", "outcome"})
	_missesCounter = prometheus.NewCounterVec(prometheus.CounterOpts{
		Subsystem: _subsystem, Name: "misses_total",
		Help: "Gets where the fast source had nothing and the origin was consulted.",
	}, []string{"name"})
	_refreshesCounter = prometheus.NewCounterVec(prometheus.CounterOpts{
		Subsystem: _subsystem, Name: "refreshes_total",
		Help: "Background refreshes, started by ServeAndRefresh, that completed successfully.",
	}, []string{"name"})
	_loadFailuresCounter = prometheus.NewCounterVec(prometheus.CounterOpts{
		Subsystem: _subsystem, Name: "load_failures_total",
		Help: "Background refreshes that returned an error. A blocking load's failure is a get outcome instead (degraded or failed).",
	}, []string{"name"})
	_fastErrorsCounter = prometheus.NewCounterVec(prometheus.CounterOpts{
		Subsystem: _subsystem, Name: "fast_errors_total",
		Help: "Calls to the fast source that returned an error rather than found=false.",
	}, []string{"name"})
	_writeBackFailuresCounter = prometheus.NewCounterVec(prometheus.CounterOpts{
		Subsystem: _subsystem, Name: "write_back_failures_total",
		Help: "Completed loads whose Set or Delete on the fast source failed. The load itself still succeeded.",
	}, []string{"name"})
	_leaseFailuresCounter = prometheus.NewCounterVec(prometheus.CounterOpts{
		Subsystem: _subsystem, Name: "lease_failures_total",
		Help: "Background refreshes whose Leaser failed to Acquire or Release, as opposed to losing an ordinary lease race.",
	}, []string{"name"})

	_collectors = []prometheus.Collector{
		_getsCounter, _missesCounter, _refreshesCounter, _loadFailuresCounter,
		_fastErrorsCounter, _writeBackFailuresCounter, _leaseFailuresCounter,
	}
)

// RegisterOption configures [Register].
type RegisterOption func(*registerConfig) error

type registerConfig struct {
	namespace string
}

// WithNamespace sets the first component of every metric name, replacing the
// default "go", so that a service's readers carry its own name.
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
// <namespace>_fallback_. Call it once at bootstrap.
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

// metrics is one reader's resolved series.
type metrics struct {
	gets                                         [5]prometheus.Counter // indexed by Outcome
	misses, refreshes, loadFailures              prometheus.Counter
	fastErrors, writeBackFailures, leaseFailures prometheus.Counter
}

func newMetrics(name string) *metrics {
	return &metrics{
		gets: [5]prometheus.Counter{
			Served:   _getsCounter.WithLabelValues(name, Served.String()),
			Loaded:   _getsCounter.WithLabelValues(name, Loaded.String()),
			Degraded: _getsCounter.WithLabelValues(name, Degraded.String()),
			Failed:   _getsCounter.WithLabelValues(name, Failed.String()),
			Aborted:  _getsCounter.WithLabelValues(name, Aborted.String()),
		},
		misses:            _missesCounter.WithLabelValues(name),
		refreshes:         _refreshesCounter.WithLabelValues(name),
		loadFailures:      _loadFailuresCounter.WithLabelValues(name),
		fastErrors:        _fastErrorsCounter.WithLabelValues(name),
		writeBackFailures: _writeBackFailuresCounter.WithLabelValues(name),
		leaseFailures:     _leaseFailuresCounter.WithLabelValues(name),
	}
}

func (m *metrics) started() {
	for _, c := range m.gets {
		c.Add(0)
	}
	m.misses.Add(0)
	m.refreshes.Add(0)
	m.loadFailures.Add(0)
	m.fastErrors.Add(0)
	m.writeBackFailures.Add(0)
	m.leaseFailures.Add(0)
}
