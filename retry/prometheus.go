package retry

import (
	"errors"
	"fmt"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/mgiaccone/keel/internal/promutil"
)

const (
	_subsystem        = "retry"
	_defaultNamespace = "go"
)

// The package's metrics. Every retrier updates them from the calling
// goroutine whether or not they are registered; [Register] publishes them
// under a namespace it prepends. With the default namespace "go":
//
//	go_retry_calls_total{retrier,backoff,result="success|exhausted|aborted|canceled|budget"}  counter
//	go_retry_attempts_total{retrier,backoff}                                                     counter, first attempts included
//	go_retry_wait_seconds_total{retrier,backoff}                                                 counter, time asked to wait
//	go_retry_hedges_total{retrier,backoff}                                                       counter, attempts WithHedge started
//	go_retry_hedge_wins_total{retrier,backoff}                                                   counter, calls a hedge won
var (
	_callsCounter = prometheus.NewCounterVec(prometheus.CounterOpts{
		Subsystem: _subsystem, Name: "calls_total",
		Help: "Calls through Do that have ended, by result. exhausted, aborted, canceled and budget returned the last attempt's error.",
	}, []string{"retrier", "backoff", "result"})
	_attemptsCounter = prometheus.NewCounterVec(prometheus.CounterOpts{
		Subsystem: _subsystem, Name: "attempts_total",
		Help: "Attempts, first attempts included; attempts minus calls is retries.",
	}, []string{"retrier", "backoff"})
	_waitCounter = prometheus.NewCounterVec(prometheus.CounterOpts{
		Subsystem: _subsystem, Name: "wait_seconds_total",
		Help: "Seconds the retrier asked to wait between attempts.",
	}, []string{"retrier", "backoff"})

	_hedgesCounter = prometheus.NewCounterVec(prometheus.CounterOpts{
		Subsystem: _subsystem, Name: "hedges_total",
		Help: "Attempts started by WithHedge because the ones in flight had not answered in time; included in attempts_total.",
	}, []string{"retrier", "backoff"})
	_hedgeWinsCounter = prometheus.NewCounterVec(prometheus.CounterOpts{
		Subsystem: _subsystem, Name: "hedge_wins_total",
		Help: "Calls whose winning attempt was a hedge; hedges that did not win were load for nothing.",
	}, []string{"retrier", "backoff"})

	_collectors = []prometheus.Collector{_callsCounter, _attemptsCounter, _waitCounter, _hedgesCounter, _hedgeWinsCounter}

	_allResults = [...]Result{Success, Exhausted, Aborted, Canceled, Budget}
)

// RegisterOption configures [Register].
type RegisterOption func(*registerConfig) error

type registerConfig struct {
	namespace string
}

// WithNamespace sets the first component of every metric name, replacing the
// default "go", so that a service's retriers carry its own name.
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
// <namespace>_retry_. Call it once at bootstrap; retriers created before or
// after it report alike. Registering twice with the same registry fails with
// prometheus.AlreadyRegisteredError.
//
// Retries per call, the number to watch, is
//
//	(rate(go_retry_attempts_total[5m]) - rate(go_retry_calls_total[5m])) / rate(go_retry_calls_total[5m])
//
// A retrier's series exist, at zero, from the moment New returns and persist
// for the life of the process.
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

// metricsObserver is the [Observer] New attaches to every retrier. Started
// resolves every series once, so the per-event methods are a single atomic
// update.
type metricsObserver struct {
	name, backoff string

	calls     [5]prometheus.Counter // indexed by Result
	attempts  prometheus.Counter
	wait      prometheus.Counter
	hedges    prometheus.Counter
	hedgeWins prometheus.Counter
}

func (o *metricsObserver) Started() {
	for _, r := range _allResults {
		o.calls[r] = _callsCounter.WithLabelValues(o.name, o.backoff, r.String())
		o.calls[r].Add(0)
	}
	o.attempts = _attemptsCounter.WithLabelValues(o.name, o.backoff)
	o.attempts.Add(0)
	o.wait = _waitCounter.WithLabelValues(o.name, o.backoff)
	o.wait.Add(0)
	o.hedges = _hedgesCounter.WithLabelValues(o.name, o.backoff)
	o.hedges.Add(0)
	o.hedgeWins = _hedgeWinsCounter.WithLabelValues(o.name, o.backoff)
	o.hedgeWins.Add(0)
}

func (o *metricsObserver) Attempt(int)                 { o.attempts.Inc() }
func (o *metricsObserver) Hedge(int)                   { o.hedges.Inc() }
func (o *metricsObserver) Wait(_ int, d time.Duration) { o.wait.Add(d.Seconds()) }
func (o *metricsObserver) Call(result Result, _ int)   { o.calls[result].Inc() }

// hedgeWon is not an [Observer] method: Call cannot say which attempt won,
// and widening the interface for one counter is not worth it. The retrier
// calls it directly; custom observers read wins from [Stats].
func (o *metricsObserver) hedgeWon() { o.hedgeWins.Inc() }
