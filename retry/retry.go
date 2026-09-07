// Package retry bounds how many times a call is attempted. It is the
// outermost of the keel primitives: a retrier wraps a breaker, which wraps a
// limiter, so each attempt is admitted, timed and counted like any other call
// and a refusal from either is never retried.
//
// # What a retry is for
//
// A retry absorbs a blip: a dropped connection, a request that hit one bad
// replica, a limiter that said "not yet". It amplifies an outage: when the
// dependency is down, every retry is another call it cannot serve, from every
// instance at once. Three things keep retries on the right side of that line,
// and this package has all three. The schedule spreads them ([Exponential],
// [Decorrelated], [Fibonacci] are all jittered). The attempt cap and the
// caller's context bound them. The budget ([WithBudget]) caps the fleet's
// retry rate as a whole, which is the only bound that holds when every
// instance is retrying; a [ratelimit] limiter is the budget.
//
// Whether a call may be retried at all is the caller's contract, not the
// retrier's: a retried write that is not idempotent is applied twice. Mark
// such errors with [Permanent], or classify them in [WithRetryIf].
//
// A retry helps when an attempt fails; a hedge ([WithHedge]) helps when an
// attempt is slow, by starting another before the first has answered. It
// needs idempotency even more than a retry, since both attempts may run to
// completion, and the budget even more, since it adds load on purpose.
//
// # Deciding what to retry
//
// An error that implements [Retryable] decides for itself. The breaker's
// refusals answer false; a limiter refusal answers true and, through
// [Delayed], says when. Everything else is asked of the [WithRetryIf]
// predicate, which retries every error by default. Neither package is
// imported here; the contract is two method signatures.
//
// # HTTP
//
// [NewTransport] wraps an http.RoundTripper so an http.Client retries
// transport errors and a set of statuses, replaying only what net/http itself
// would replay.
//
// [Stats.String] renders one log line, [WithOnRetry] hooks every retry,
// [WithObserver] streams every event, and every retrier feeds the package's
// Prometheus metrics under its name, published by [Register].
package retry

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"time"
)

// Result is how a call through [Retrier.Do] ended.
type Result uint8

const (
	// Success: an attempt returned a nil error.
	Success Result = iota
	// Exhausted: the attempt cap was reached; the last error was returned.
	Exhausted
	// Aborted: an attempt returned an error that must not be retried, by
	// [Permanent], by [Retryable], by [WithRetryIf], or because the delay it
	// asked for exceeds [WithMaxRetryAfter].
	Aborted
	// Canceled: the caller's context ended before an attempt or during a
	// wait.
	Canceled
	// Budget: the [WithBudget] veto refused the retry.
	Budget
)

// String returns "success", "exhausted", "aborted", "canceled" or "budget".
func (r Result) String() string {
	switch r {
	case Success:
		return "success"
	case Exhausted:
		return "exhausted"
	case Aborted:
		return "aborted"
	case Canceled:
		return "canceled"
	case Budget:
		return "budget"
	default:
		return fmt.Sprintf("Result(%d)", uint8(r))
	}
}

// ErrInvalidOption is wrapped by every error a constructor returns for a
// value that cannot be meant.
var ErrInvalidOption = errors.New("retry: invalid option")

// Retryable is implemented by errors that decide for themselves whether a
// retry may help. It takes precedence over [WithRetryIf]. The breaker's
// refusals report false, a limiter's refusal true; [Permanent] wraps any error
// so that it reports false.
type Retryable interface {
	Retryable() bool
}

// Delayed is implemented by errors that know when a retry may succeed, such
// as a limiter refusal or an HTTP response with Retry-After. The retrier
// waits at least that long, plus the schedule's own jittered delay so a fleet
// told the same value does not retry together. A delay above
// [WithMaxRetryAfter] makes the error terminal instead.
type Delayed interface {
	RetryDelay() time.Duration
}

// Permanent marks err as not to be retried: a validation rejection, a
// not-found, a write that must not be replayed. Do stops on it and returns
// err itself, unwrapped, when Permanent is the outermost wrapper; wrapped
// deeper, it still stops the retrier and errors.Is and errors.As still reach
// err. Permanent(nil) is nil.
func Permanent(err error) error {
	if err == nil {
		return nil
	}
	return &permanentError{err: err}
}

type permanentError struct{ err error }

func (e *permanentError) Error() string   { return e.err.Error() }
func (e *permanentError) Unwrap() error   { return e.err }
func (e *permanentError) Retryable() bool { return false }

// Observer receives the retrier's events, on the goroutine running the call,
// in the order they happen: Started once from [New]; Attempt before every
// attempt, numbered from 1; Hedge right after the Attempt of an attempt that
// [WithHedge] started, and never otherwise; Wait before every wait, with the
// number of attempts that have failed so far, which is the retry number
// without hedging; Call once when the call ends, with how many attempts it
// took. Implementations must return promptly. The Prometheus metrics are one
// implementation.
type Observer interface {
	Started()
	Attempt(attempt int)
	Hedge(attempt int)
	Wait(failed int, d time.Duration)
	Call(result Result, attempts int)
}

// Option configures a [Retrier]. An option handed a value that cannot be
// meant makes [New] fail with an error wrapping [ErrInvalidOption]; New
// reports every invalid option, not just the first.
type Option func(*config) error

type config struct {
	name          string
	backoff       Backoff
	maxAttempts   int
	maxRetryAfter time.Duration
	hedge         time.Duration // 0 = off
	retryIf       func(error) bool
	budget        func(context.Context) error
	onRetry       func(attempt int, err error, delay time.Duration)
	observers     []Observer
	now           func() time.Time
	sleep         func(context.Context, time.Duration) error
	seed          [2]uint64
}

// WithMaxAttempts caps the attempts per call, the first included. Default 3.
// 1 disables retries, which is a legitimate way to keep the accounting and the
// HTTP transport while retrying nothing. The cap is the second bound after the
// caller's context; without a deadline on the context, a cap of n against a
// dependency that hangs costs n times the attempt's own timeout.
func WithMaxAttempts(n int) Option {
	return func(c *config) error {
		if n < 1 {
			return fmt.Errorf("%w: WithMaxAttempts(%d): must be at least 1", ErrInvalidOption, n)
		}
		c.maxAttempts = n
		return nil
	}
}

// WithRetryIf sets the predicate for errors that do not implement
// [Retryable]. Default: every such error is retried. Replace it for any real
// dependency, because the default retries answers as well as failures: a
// validation rejection or a not-found is retried until the cap and fails
// exactly the same way, slower. Return true only for errors that a fresh
// attempt could change: connection failures, timeouts, 5xx.
func WithRetryIf(fn func(error) bool) Option {
	return func(c *config) error {
		if fn == nil {
			return fmt.Errorf("%w: WithRetryIf(nil)", ErrInvalidOption)
		}
		c.retryIf = fn
		return nil
	}
}

// WithMaxRetryAfter caps the delay a [Delayed] error may ask for. Default
// 30s. A limiter refusal always fits; a remote Retry-After of an hour does
// not, and the call ends as [Aborted] rather than holding a goroutine and a
// request's memory for an hour.
func WithMaxRetryAfter(d time.Duration) Option {
	return func(c *config) error {
		if d <= 0 {
			return fmt.Errorf("%w: WithMaxRetryAfter(%s): must be positive", ErrInvalidOption, d)
		}
		c.maxRetryAfter = d
		return nil
	}
}

// WithHedge starts another attempt when the ones in flight have not answered
// after the given delay, up to [WithMaxAttempts]. The first success wins; the
// rest have their contexts cancelled and their results discarded. Off by
// default.
//
// fn must be safe to run more than once concurrently with itself: a hedge can
// have two attempts in flight at the same time, so a non-idempotent handler
// can double-apply without either attempt having failed — a call a sequential
// retry would never make, since there a second attempt only ever follows a
// first that has already ended. Mark the operation safe with an idempotency
// key the caller generates once per operation and sends on every attempt; the
// HTTP transport already treats Idempotency-Key and X-Idempotency-Key as a
// replay signal. This package neither generates nor stores such a key — that
// is the caller's and the server's contract, not this package's.
//
// Cancelling a loser's context is best effort, not a guarantee: it can stop
// fn from starting more work, but it cannot unsend a request already on the
// wire, so a losing attempt's write may still reach and be applied by the
// dependency after Do has returned with a different attempt's answer. A loser
// that itself succeeds is not distinguished from one that failed or was
// cancelled — its result is discarded and, on the HTTP transport, its
// response body is drained and closed — so the caller has no way to learn
// that a losing write landed.
//
// A retry helps when an attempt fails; a hedge helps when an attempt is slow.
// It cuts the latency tail that a few slow replicas cause, at the cost of
// extra load, and against a dependency that is uniformly slow it only doubles
// that load. Hedge only with a budget.
//
// A hedge asks the budget like a retry, and a refusal ends every further
// attempt of the call: the attempts in flight finish and decide it. Hedges
// count against the attempt cap together with retries. A failure while other
// attempts are running starts nothing; once every attempt in flight has
// failed, the ordinary retry path applies with the schedule's wait, which the
// hedge delay does not floor, and the schedule is asked for the wait after
// that many failed attempts, hedges included. A failure that must not be
// retried, or the caller's context ending, cancels the other attempts and
// ends the call at once.
//
// Each attempt runs on a context derived from the caller's. The context of
// the attempt whose result Do returns, the winner or the last failure, is
// left alive, since the value it produced may keep using it after Do
// returns, an HTTP body for one, and it ends with the caller's; every other
// attempt's ends when the call does. Hedge under a per-call context: under a
// service-lifetime cancellable context, every returned attempt's context
// stays registered in it until it ends.
func WithHedge(after time.Duration) Option {
	return func(c *config) error {
		if after <= 0 {
			return fmt.Errorf("%w: WithHedge(%s): must be positive", ErrInvalidOption, after)
		}
		c.hedge = after
		return nil
	}
}

// WithBudget sets a veto asked before every retry and every hedge, never
// before the first attempt. A non-nil error ends the call as [Budget],
// returning the last error from fn; the veto's own error goes to the hook and
// the observers. It takes the shape ratelimit.Admission returns, so
//
//	retry.WithBudget(ratelimit.AdmissionGlobal(limiter))
//
// caps retries at the limiter's rate, per process with a memory store or
// fleet-wide with Redis. This is the bound that matters during an outage:
// with every instance retrying, a budget of a tenth of normal traffic keeps
// the retries from being the load that prevents recovery.
func WithBudget(fn func(context.Context) error) Option {
	return func(c *config) error {
		if fn == nil {
			return fmt.Errorf("%w: WithBudget(nil)", ErrInvalidOption)
		}
		c.budget = fn
		return nil
	}
}

// WithOnRetry sets a hook called before every wait with the attempt that just
// failed, its error, and the coming delay. It is the place to log retries. It
// runs on the calling goroutine, so it adds its own latency to the call. nil
// removes a previously set hook.
func WithOnRetry(fn func(attempt int, err error, delay time.Duration)) Option {
	return func(c *config) error {
		c.onRetry = fn
		return nil
	}
}

// WithObserver attaches an observer; see [Observer]. It may be given more
// than once, and observers are notified in the order they were attached.
func WithObserver(o Observer) Option {
	return func(c *config) error {
		if o == nil {
			return fmt.Errorf("%w: WithObserver(nil)", ErrInvalidOption)
		}
		c.observers = append(c.observers, o)
		return nil
	}
}

// WithClock sets the clock used to turn an HTTP-date Retry-After into a
// delay. Default time.Now.
func WithClock(now func() time.Time) Option {
	return func(c *config) error {
		if now == nil {
			return fmt.Errorf("%w: WithClock(nil)", ErrInvalidOption)
		}
		c.now = now
		return nil
	}
}

// WithSleep sets how the retrier waits. Default: a timer, returning ctx.Err()
// if the context ends first. Tests replace it to record the waits and return
// immediately.
func WithSleep(sleep func(ctx context.Context, d time.Duration) error) Option {
	return func(c *config) error {
		if sleep == nil {
			return fmt.Errorf("%w: WithSleep(nil)", ErrInvalidOption)
		}
		c.sleep = sleep
		return nil
	}
}

// WithSeed seeds the jitter RNG. The default, (0, 0), seeds it from the
// runtime; any other pair makes the schedule's draws reproducible.
func WithSeed(a, b uint64) Option {
	return func(c *config) error {
		c.seed = [2]uint64{a, b}
		return nil
	}
}

func defaultSleep(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return ctx.Err()
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func defaultRetryIf(error) bool { return true }

// Retrier runs a call until it succeeds or a bound says stop. Create one with
// [New]; the zero value is not usable. It has no goroutine and nothing to
// stop; its counters are atomics and it is safe for concurrent use.
type Retrier struct {
	cfg     config
	metrics *metricsObserver // the first observer; hedge wins reach it directly

	mu  sync.Mutex // guards rng, which is not safe for concurrent use
	rng *rand.Rand

	attempts                                        atomic.Uint64
	succeeded, exhausted, aborted, canceled, budget atomic.Uint64
	hedges, hedgeWins                               atomic.Uint64
	waited                                          atomic.Int64
}

// New returns a retrier running backoff. name identifies it in [Stats] and
// as the retrier label of its metrics. If name is empty, backoff is nil or
// invalid, or any option is invalid, New returns an error wrapping
// [ErrInvalidOption] that describes every problem.
func New(name string, backoff Backoff, opts ...Option) (*Retrier, error) {
	cfg := config{
		name:          name,
		backoff:       backoff,
		maxAttempts:   3,
		maxRetryAfter: 30 * time.Second,
		retryIf:       defaultRetryIf,
		now:           time.Now,
		sleep:         defaultSleep,
	}
	var errs []error
	if name == "" {
		errs = append(errs, fmt.Errorf("%w: New: name must not be empty", ErrInvalidOption))
	}
	if backoff == nil {
		errs = append(errs, fmt.Errorf("%w: New: backoff must not be nil", ErrInvalidOption))
	} else if err := backoff.Validate(); err != nil {
		errs = append(errs, err)
	}
	for _, opt := range opts {
		if err := opt(&cfg); err != nil {
			errs = append(errs, err)
		}
	}
	if err := errors.Join(errs...); err != nil {
		return nil, err
	}

	metrics := &metricsObserver{name: name, backoff: backoff.Name()}
	cfg.observers = append([]Observer{metrics}, cfg.observers...)
	if cfg.seed == [2]uint64{} {
		cfg.seed = [2]uint64{rand.Uint64(), rand.Uint64()}
	}

	r := &Retrier{cfg: cfg, metrics: metrics, rng: rand.New(rand.NewPCG(cfg.seed[0], cfg.seed[1]))}
	for _, o := range cfg.observers {
		o.Started()
	}
	return r, nil
}

// Do runs fn until it returns a nil error or a bound says stop, and returns
// what the last attempt returned. The error is never rewritten: on
// [Exhausted], [Canceled] and [Budget] it is the last error from fn; on
// [Aborted] it is the error fn returned, with a [Permanent] wrapper removed
// when it is the outermost one. The one exception is a context that is
// already done before fn ever ran, where Do returns ctx.Err().
//
// Each attempt receives ctx as given, or a context derived from it under
// [WithHedge], where attempts run concurrently and the losers are cancelled.
// Bound the attempt, not the call: wrap fn in a breaker with WithTimeout, or
// derive a per-attempt context inside fn. A deadline on ctx bounds the whole
// call, waits included.
//
// A panic in fn propagates to the caller; under WithHedge the other attempts
// are cancelled first, and a losing attempt that panics after the call has
// returned panics on its own goroutine rather than being swallowed.
//
// Because Do may hand fn to other goroutines under WithHedge, a closure that
// captures variables is heap-allocated whether or not hedging is on; the
// retrier itself allocates nothing on the success path without WithHedge.
//
// Do is a generic method and therefore cannot be part of an interface; put a
// non-generic adapter in front of it if one is needed.
func (r *Retrier) Do[T any](ctx context.Context, fn func(context.Context) (T, error)) (T, error) {
	return do(r, ctx, fn, nil)
}

// do is Do with a hook for the results the call does not return: the failed
// result a retry supersedes, and the losers of a hedge race. The transport
// uses it to close response bodies the caller never sees. discard may be nil.
func do[T any](r *Retrier, ctx context.Context, fn func(context.Context) (T, error), discard func(T, error)) (T, error) {
	if r.cfg.hedge > 0 {
		return hedged(r, ctx, fn, discard)
	}
	var (
		v        T
		last     error
		previous time.Duration // the schedule's last delay, for Decorrelated
		attempts int
	)
	for {
		if ctx.Err() != nil {
			r.end(Canceled, attempts)
			if attempts == 0 {
				return v, ctx.Err()
			}
			return v, unwrapPermanent(last)
		}

		if attempts > 0 && discard != nil {
			discard(v, last) // superseded by the attempt about to start
		}
		attempts++
		r.attempts.Add(1)
		for _, o := range r.cfg.observers {
			o.Attempt(attempts)
		}

		v, last = fn(ctx)
		if last == nil {
			r.end(Success, attempts)
			return v, nil
		}

		if ctx.Err() != nil {
			r.end(Canceled, attempts)
			return v, unwrapPermanent(last)
		}
		retryable, floor := r.classify(last)
		switch {
		case !retryable, floor > r.cfg.maxRetryAfter:
			r.end(Aborted, attempts)
			return v, unwrapPermanent(last)
		case attempts >= r.cfg.maxAttempts:
			r.end(Exhausted, attempts)
			return v, unwrapPermanent(last)
		}

		if r.cfg.budget != nil {
			if err := r.cfg.budget(ctx); err != nil {
				if r.cfg.onRetry != nil {
					r.cfg.onRetry(attempts, err, 0)
				}
				r.end(Budget, attempts)
				return v, unwrapPermanent(last)
			}
		}
		previous = r.cfg.backoff.Delay(attempts, previous, r.uniform())
		wait := floor + previous
		if r.cfg.onRetry != nil {
			r.cfg.onRetry(attempts, last, wait)
		}
		for _, o := range r.cfg.observers {
			o.Wait(attempts, wait)
		}
		r.waited.Add(int64(wait))
		if err := r.cfg.sleep(ctx, wait); err != nil {
			r.end(Canceled, attempts)
			return v, unwrapPermanent(last)
		}
	}
}

// classify asks the error, then the predicate, and reads any delay it asks
// for.
func (r *Retrier) classify(err error) (retryable bool, floor time.Duration) {
	var d Delayed
	if errors.As(err, &d) {
		floor = max(d.RetryDelay(), 0)
	}
	var re Retryable
	if errors.As(err, &re) {
		return re.Retryable(), floor
	}
	return r.cfg.retryIf(err), floor
}

func (r *Retrier) uniform() float64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.rng.Float64()
}

func (r *Retrier) end(result Result, attempts int) {
	switch result {
	case Success:
		r.succeeded.Add(1)
	case Exhausted:
		r.exhausted.Add(1)
	case Aborted:
		r.aborted.Add(1)
	case Canceled:
		r.canceled.Add(1)
	case Budget:
		r.budget.Add(1)
	}
	for _, o := range r.cfg.observers {
		o.Call(result, attempts)
	}
}

func unwrapPermanent(err error) error {
	if p, ok := err.(*permanentError); ok {
		return p.err
	}
	return err
}

// Stats is a snapshot of a [Retrier]. Calls counts calls that have ended and
// is the sum of the five result counters, so
//
//	Succeeded + Exhausted + Aborted + Canceled + BudgetDenied == Calls
//
// holds at every observation. Attempts >= Calls, since a call's attempts are
// counted before it ends; Hedged <= Attempts - Calls, since a hedge is never
// a first attempt; and HedgeWon <= min(Hedged, Succeeded). Those three hold
// exactly for a retrier with no call in flight; the counters are independent
// atomics read one after another, not a locked snapshot, so with calls in
// flight each may be off by the calls that ended while Stats was reading.
// Every counter is monotonic.
type Stats struct {
	// Name and Backoff identify the retrier.
	Name, Backoff string
	// HedgeAfter is the [WithHedge] delay; 0 when hedging is off.
	HedgeAfter time.Duration
	// Calls is calls through Do that have ended, by any result.
	Calls uint64
	// Attempts is invocations of fn, first attempts included.
	Attempts uint64
	// Succeeded, Exhausted, Aborted, Canceled and BudgetDenied count calls
	// by [Result].
	Succeeded, Exhausted, Aborted, Canceled, BudgetDenied uint64
	// Hedged is attempts started by [WithHedge]; HedgeWon is calls whose
	// winning attempt was one of them.
	Hedged, HedgeWon uint64
	// Waited is the total time the retrier has asked to wait, whether or not
	// a wait ran to completion. Hedge delays are not waits.
	Waited time.Duration
}

// String renders the snapshot as one log line:
//
//	retry: name=db backoff=exponential calls=812 attempts=901 ok=800 exhausted=5 aborted=7 canceled=0 budget=0 waited=1m2s hedged=40 hedge_won=31
//
// hedged and hedge_won are present only under [WithHedge].
func (s Stats) String() string {
	buf := fmt.Appendf(make([]byte, 0, 160), "retry: name=%s backoff=%s calls=%d attempts=%d ok=%d exhausted=%d aborted=%d canceled=%d budget=%d waited=%s",
		s.Name, s.Backoff, s.Calls, s.Attempts, s.Succeeded, s.Exhausted, s.Aborted, s.Canceled, s.BudgetDenied, s.Waited)
	if s.HedgeAfter > 0 {
		buf = fmt.Appendf(buf, " hedged=%d hedge_won=%d", s.Hedged, s.HedgeWon)
	}
	return string(buf)
}

// Stats returns a snapshot.
func (r *Retrier) Stats() Stats {
	s := Stats{
		Name: r.cfg.name, Backoff: r.cfg.backoff.Name(), HedgeAfter: r.cfg.hedge,
		Attempts:  r.attempts.Load(),
		Succeeded: r.succeeded.Load(), Exhausted: r.exhausted.Load(), Aborted: r.aborted.Load(),
		Canceled: r.canceled.Load(), BudgetDenied: r.budget.Load(),
		Hedged: r.hedges.Load(), HedgeWon: r.hedgeWins.Load(),
		Waited: time.Duration(r.waited.Load()),
	}
	s.Calls = s.Succeeded + s.Exhausted + s.Aborted + s.Canceled + s.BudgetDenied
	return s
}
