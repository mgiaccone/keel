// Package breaker implements a circuit breaker whose entire mutable state is
// owned by a single goroutine and reached only through channels: the core has
// no mutexes, no atomics and no timers.
//
// The breaker counts consecutive failures, which suits low-volume guarded
// paths such as a fallback that runs only on a cache miss. On a high-volume
// path, where scattered errors are normal, use adaptive throttling instead.
//
// Time-based transitions (open → half-open) are evaluated lazily against an
// injectable clock when a call or an inspection arrives, never by a timer.
//
// [Stats.String] renders one log line, [WithOnStateChange] hooks every
// transition, [WithObserver] streams every event to custom instrumentation,
// and every breaker feeds the package's Prometheus metrics under its name,
// published by [Register].
package breaker

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"runtime"
	"time"
)

// State is the position of the circuit.
type State uint8

const (
	// Closed is the healthy state: calls are admitted and failures counted.
	Closed State = iota
	// Open means the backend is presumed down: calls are rejected with
	// [ErrOpen] without invoking fn, until the open interval has elapsed.
	Open
	// HalfOpen admits up to MaxProbes trial calls; SuccessThreshold
	// consecutive successes close the circuit, a single failure reopens it.
	HalfOpen
)

// String returns "closed", "open" or "half-open".
func (s State) String() string {
	switch s {
	case Closed:
		return "closed"
	case Open:
		return "open"
	case HalfOpen:
		return "half-open"
	default:
		return fmt.Sprintf("State(%d)", uint8(s))
	}
}

var (
	// ErrOpen is returned by [Breaker.Do] while the circuit is open. fn was
	// not invoked. Callers should fail fast and not retry.
	ErrOpen = errors.New("breaker: circuit open")
	// ErrProbeLimit is returned by [Breaker.Do] while the circuit is
	// half-open and MaxProbes trial calls are already in flight. fn was not
	// invoked. Callers should fail fast and not retry.
	ErrProbeLimit = errors.New("breaker: half-open probe limit reached")
	// ErrBulkhead is returned by [Breaker.Do] when the in-flight limit set by
	// [WithMaxInFlight] or [WithAdaptiveInFlight] is reached. fn was not
	// invoked. Callers should fail fast and not retry.
	ErrBulkhead = errors.New("breaker: too many calls in flight")
	// ErrInvalidOption is wrapped by every error [New] returns for an option
	// value that cannot be meant, such as a threshold below 1.
	ErrInvalidOption = errors.New("breaker: invalid option")
	// ErrStopped is returned by [Breaker.Do] after [Breaker.Stop].
	ErrStopped = errors.New("breaker: stopped")
)

// Option configures a [Breaker]. Every setting has a documented default, so
// New() with no options is a usable breaker; in practice you will want
// [WithIsFailure] for your backend.
//
// An option that is handed a value that cannot be meant (a threshold below 1,
// a negative interval, a jitter outside [0, 1]) makes [New] fail with an error
// wrapping [ErrInvalidOption]. New reports every invalid option, not just the
// first.
type Option func(*config) error

// config is the resolved configuration. It is built by New and never changes
// afterwards.
type config struct {
	name             string
	failureThreshold int
	successThreshold int
	maxProbes        int
	openBase         time.Duration
	openMax          time.Duration
	openJitter       float64
	timeout          time.Duration
	maxInFlight      int   // static bulkhead; 0 = unlimited
	aimd             *aimd // adaptive bulkhead; nil = off
	ramp             *ramp // recovery ramp; nil = off
	admission        func(context.Context) error
	isFailure        func(error) bool
	onStateChange    func(from, to State)
	observers        []Observer
	now              func() time.Time
	seed             [2]uint64
}

// WithFailureThreshold sets the number of consecutive failures, while closed,
// that trips the circuit open. A success resets the run. Default 5.
func WithFailureThreshold(n int) Option {
	return func(c *config) error {
		if n < 1 {
			return fmt.Errorf("%w: WithFailureThreshold(%d): must be at least 1", ErrInvalidOption, n)
		}
		c.failureThreshold = n
		return nil
	}
}

// WithSuccessThreshold sets the number of consecutive successful probes,
// while half-open, needed to close the circuit. Default 2, deliberately not 1.
//
// With 1, a single lucky probe against a still-sick backend restores full
// traffic, which fails, which reopens the circuit: the classic flap. Two
// consecutive successes are cheap insurance against it. A failed probe reopens
// the circuit regardless of how many successes preceded it.
func WithSuccessThreshold(n int) Option {
	return func(c *config) error {
		if n < 1 {
			return fmt.Errorf("%w: WithSuccessThreshold(%d): must be at least 1", ErrInvalidOption, n)
		}
		c.successThreshold = n
		return nil
	}
}

// WithMaxProbes sets the number of trial calls admitted concurrently while
// half-open. Further calls are rejected with [ErrProbeLimit] until a probe
// settles. Default 1.
func WithMaxProbes(n int) Option {
	return func(c *config) error {
		if n < 1 {
			return fmt.Errorf("%w: WithMaxProbes(%d): must be at least 1", ErrInvalidOption, n)
		}
		c.maxProbes = n
		return nil
	}
}

// WithOpenInterval sets the open interval after the first trip (base) and its
// cap (max). Defaults 5s and 60s.
//
// The interval doubles per consecutive trip (a trip that follows a half-open
// period that did not lead to a close), so a backend that stays down is probed
// less and less often: base, 2×base, 4×base… capped at max. ConsecutiveTrips,
// and therefore the interval, resets when the circuit closes.
func WithOpenInterval(base, max time.Duration) Option {
	return func(c *config) error {
		if base <= 0 || max < base {
			return fmt.Errorf("%w: WithOpenInterval(%s, %s): need 0 < base <= max", ErrInvalidOption, base, max)
		}
		c.openBase, c.openMax = base, max
		return nil
	}
}

// WithOpenJitter sets the fraction by which each open interval is randomised:
// the computed interval is multiplied by a value drawn uniformly from
// [1-fraction, 1+fraction]. Default 0.2. Zero disables jitter.
//
// Jitter prevents lockstep probing. Every instance of a service trips on the
// same backend outage at nearly the same moment; without jitter they all
// finish the same open interval together and probe at once, so the recovering
// backend takes a synchronised burst, fails under it, and every instance
// reopens together for twice as long. Jitter spreads the probes so recovery
// is gradual.
//
// The interval is drawn once, on entering Open, and is fixed for that open
// period; NextProbeIn in [Stats] counts down to it.
func WithOpenJitter(fraction float64) Option {
	return func(c *config) error {
		if fraction < 0 || fraction > 1 || fraction != fraction {
			return fmt.Errorf("%w: WithOpenJitter(%v): must be in [0, 1]", ErrInvalidOption, fraction)
		}
		c.openJitter = fraction
		return nil
	}
}

// WithTimeout bounds each admitted call: fn receives a context that expires
// after d, or sooner if the caller's own context does. Default none, in which
// case only the caller's context bounds the call.
//
// Set it. The breaker assumes every admitted call eventually settles; a call
// that hangs while the circuit is half-open holds the probe slot and nothing
// can free it. With a timeout, a hung backend becomes a
// context.DeadlineExceeded, which IsFailure counts as a failure by default, so
// the circuit trips instead of wedging. The timeout starts when the call is
// admitted, not when Do is entered.
//
// fn must honour its context. The breaker cannot abort fn; Do returns only
// when fn does, whatever the timeout says, so a fn that ignores cancellation
// defeats the option.
func WithTimeout(d time.Duration) Option {
	return func(c *config) error {
		if d <= 0 {
			return fmt.Errorf("%w: WithTimeout(%s): must be positive", ErrInvalidOption, d)
		}
		c.timeout = d
		return nil
	}
}

// WithMaxInFlight caps the calls in flight at once: a bulkhead. A call
// arriving at the cap is rejected immediately with [ErrBulkhead]; nothing
// queues. Default unlimited.
//
// The bulkhead protects the caller, not the backend. A backend that is slow
// but not failing never trips the circuit, and every in-flight call holds a
// goroutine, a connection and whatever the request allocated; without a cap
// those grow until requests that never needed the backend fail too. Size it
// from Little's law, in-flight = rate × latency at the p99, times three to
// five so it never bites in normal operation, and keep it below the
// connection pool size. Worst-case exposure is the cap times [WithTimeout].
//
// While half-open, MaxProbes is the tighter limit and applies first. Mutually
// exclusive with [WithAdaptiveInFlight].
func WithMaxInFlight(n int) Option {
	return func(c *config) error {
		if n < 1 {
			return fmt.Errorf("%w: WithMaxInFlight(%d): must be at least 1", ErrInvalidOption, n)
		}
		c.maxInFlight = n
		return nil
	}
}

// WithAdaptiveInFlight is a bulkhead whose cap moves between min and max by
// additive increase, multiplicative decrease (AIMD), the control rule TCP uses
// for congestion: each successful call that completes within target raises
// the cap by one, each failure or call slower than target halves it. The cap
// starts at max, so the limiter only ever tightens on evidence and relaxes
// back as the backend recovers. Cancelled calls leave it unchanged.
//
// Adaptive limits need volume: on a path doing a few calls per second, a
// single slow query is indistinguishable from saturation and the cap will
// jitter. Prefer [WithMaxInFlight] there. The current cap is visible as
// Stats.InFlightLimit and the in_flight_limit gauge. Mutually exclusive with
// WithMaxInFlight.
func WithAdaptiveInFlight(min, max int, target time.Duration) Option {
	return func(c *config) error {
		if min < 1 || max < min || target <= 0 {
			return fmt.Errorf("%w: WithAdaptiveInFlight(%d, %d, %s): need 1 <= min <= max and target > 0", ErrInvalidOption, min, max, target)
		}
		c.aimd = &aimd{min: min, max: max, target: target}
		return nil
	}
}

// aimd is the adaptive bulkhead policy.
type aimd struct {
	min, max int
	target   time.Duration
}

// WithRecoveryRamp makes traffic return gradually after a recovery. When the
// circuit closes, the in-flight cap is set to start and grows by one per
// successful call until it reaches end; then the steady-state limit resumes:
// [WithMaxInFlight]'s cap, or unlimited. Default off, in which case closing
// releases the full backlog onto a backend that has just proven it can
// handle one probe at a time.
//
// Failures do not move the ramp; enough of them reopen the circuit as usual
// and the ramp restarts on the next close. Calls refused during the ramp get
// [ErrBulkhead]. Nothing happens at startup: the breaker begins closed at its
// steady cap. Stats.Ramping and the ramping gauge show a ramp in progress,
// which matters because a cap of 3 during a ramp is recovery working and the
// same cap under [WithAdaptiveInFlight] is the backend struggling.
//
// With WithAdaptiveInFlight the ramp only seeds the cap at start; AIMD's own
// rule grows it from there, and the ramp is over when the cap reaches end or
// AIMD lowers it. Validation: 1 <= start <= end; end <= the static cap if
// one is set; start >= min and end <= max under the adaptive bulkhead.
func WithRecoveryRamp(start, end int) Option {
	return func(c *config) error {
		if start < 1 || end < start {
			return fmt.Errorf("%w: WithRecoveryRamp(%d, %d): need 1 <= start <= end", ErrInvalidOption, start, end)
		}
		c.ramp = &ramp{start: start, end: end}
		return nil
	}
}

// ramp is the recovery ramp policy.
type ramp struct {
	start, end int
}

// WithAdmission sets a veto that runs on the caller's goroutine after the
// circuit has admitted a call and before fn is invoked. If it returns an
// error, fn does not run, Do returns that error unchanged, and the call is
// recorded as denied: it counts in Stats.Denied and as result="denied", not as
// a failure, and it does not touch the circuit's failure run, the ramp or the
// adaptive bulkhead.
//
// This is the seam for anything that must approve a call and may block or do
// I/O to decide, which the state goroutine must never do: a rate limiter,
// local or distributed, a quota check, an authorisation check. It runs after
// admission so that an open circuit still answers ErrOpen and a call the
// circuit would have refused never consumes the veto's quota. While it runs
// the call holds its in-flight slot, so its latency is bounded by the
// bulkhead like any other part of the call.
func WithAdmission(fn func(context.Context) error) Option {
	return func(c *config) error {
		if fn == nil {
			return fmt.Errorf("%w: WithAdmission(nil)", ErrInvalidOption)
		}
		c.admission = fn
		return nil
	}
}

// WithIsFailure sets the predicate that decides whether an error returned by
// fn counts against the circuit. It is the most consequential setting and
// should be set for every backend.
//
// Default: err != nil && !errors.Is(err, context.Canceled). That is, any error
// other than a caller-side cancellation is a failure, including
// context.DeadlineExceeded, since a backend that is too slow to answer is
// exactly what the breaker is meant to detect.
//
// The default is wrong for most real backends because it counts every error.
// Errors that mean "the backend answered correctly and the answer was no" —
// sql.ErrNoRows, an HTTP 404, a validation rejection, a not-found from a
// lookup — are successes as far as the circuit is concerned: the backend is
// healthy, it just does not have the row. Counting them as failures is the
// most common way to make a breaker trip on perfectly healthy traffic, and no
// other setting can compensate for it. Your predicate should return true only
// for errors that indicate the backend itself is unhealthy: timeouts,
// connection refusals, 5xx.
//
// An error for which the predicate returns false but which wraps
// context.Canceled is treated as neutral: it neither counts as a failure nor
// as a success, because a caller giving up says nothing about the backend. It
// still appears in Stats.Canceled. If the predicate returns true for a
// cancellation, it is a failure.
func WithIsFailure(fn func(err error) bool) Option {
	return func(c *config) error {
		if fn == nil {
			return fmt.Errorf("%w: WithIsFailure(nil)", ErrInvalidOption)
		}
		c.isFailure = fn
		return nil
	}
}

// WithOnStateChange sets a hook called synchronously from the state goroutine
// on every transition, after the new state is in effect and before the call
// that caused it is answered. It must return promptly and must not call back
// into the Breaker: State, Stats, Do and Stop would all deadlock because the
// goroutine that answers them is the one running the hook.
//
// This is the natural place to log transitions, together with the name of
// the guarded dependency. nil removes a previously set hook. The hook must
// not capture the Breaker it is attached to: besides the deadlock, a
// reference from the breaker's own configuration back to its handle would
// keep the handle reachable forever.
func WithOnStateChange(fn func(from, to State)) Option {
	return func(c *config) error {
		c.onStateChange = fn
		return nil
	}
}

// Result is what became of a call that reached the breaker, as reported to
// an [Observer].
type Result uint8

const (
	// Success: fn ran and IsFailure said no.
	Success Result = iota
	// Failure: fn ran and IsFailure said yes.
	Failure
	// Canceled: fn ran and returned a context.Canceled that IsFailure did not
	// classify as a failure; neutral for the circuit.
	Canceled
	// Rejected: fn did not run; the call got ErrOpen or ErrProbeLimit.
	Rejected
	// Shed: fn did not run; the call got ErrBulkhead.
	Shed
	// Denied: fn did not run; the [WithAdmission] veto returned an error.
	Denied
)

// String returns "success", "failure", "canceled", "rejected", "shed" or
// "denied".
func (r Result) String() string {
	switch r {
	case Success:
		return "success"
	case Failure:
		return "failure"
	case Canceled:
		return "canceled"
	case Rejected:
		return "rejected"
	case Shed:
		return "shed"
	case Denied:
		return "denied"
	default:
		return fmt.Sprintf("Result(%d)", uint8(r))
	}
}

// Observer receives the breaker's events. Every method except Started is
// called synchronously from the state goroutine, so implementations must
// return promptly and must not call back into the Breaker. Events arrive in the order
// they happened, which makes an Observer the right place to maintain metrics:
// counters incremented here agree exactly with [Stats].
//
// Started is called once, from [New] before it returns, and Stopped once when
// the state goroutine exits, on [Breaker.Stop] or after the Breaker has become
// unreachable and been collected; nothing is delivered after Stopped. Call is delivered once per call that reached the breaker,
// after its result is counted and before any transition it causes, including
// results of calls admitted before a transition (they are counted, not acted
// on). Transition is delivered on every state change; openUntil is the
// deadline of the new open period when to is Open and the zero time
// otherwise. Load is delivered whenever the number of calls in flight, the
// in-flight limit or the ramping flag changes; limit is 0 when unlimited.
type Observer interface {
	Started()
	Call(Result)
	Transition(from, to State, consecutiveTrips int, openUntil time.Time)
	Load(inFlight, limit int, ramping bool)
	Stopped()
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

// WithClock sets the clock used for open-interval deadlines. Default
// time.Now. A fake clock makes open → half-open transitions deterministic in
// tests.
func WithClock(now func() time.Time) Option {
	return func(c *config) error {
		if now == nil {
			return fmt.Errorf("%w: WithClock(nil)", ErrInvalidOption)
		}
		c.now = now
		return nil
	}
}

// WithSeed seeds the jitter RNG. By default it is seeded from the runtime; a
// fixed seed makes jittered intervals reproducible.
func WithSeed(a, b uint64) Option {
	return func(c *config) error {
		c.seed = [2]uint64{a, b}
		return nil
	}
}

func defaultIsFailure(err error) bool {
	return err != nil && !errors.Is(err, context.Canceled)
}

func defaultConfig() config {
	return config{
		failureThreshold: 5,
		successThreshold: 2,
		maxProbes:        1,
		openBase:         5 * time.Second,
		openMax:          60 * time.Second,
		openJitter:       0.2,
		isFailure:        defaultIsFailure,
		now:              time.Now,
	}
}

// Stats is a snapshot of the breaker, laid out to be read by a person during
// an incident. All counters are cumulative since New.
//
// Invariants, which hold at every observation:
//
//	Admitted + Rejected + Shed + Denied + InFlight == Calls
//	Successes + Failures + Canceled == Admitted
//
// Every cumulative counter is monotonic; InFlight is the only field that
// moves both ways.
type Stats struct {
	// Name is the breaker's name, as given to [New].
	Name string
	// State is the current position of the circuit, with any due
	// open → half-open transition already applied.
	State State
	// ConsecutiveTrips is the number of trips since the circuit last closed.
	// It drives the exponential open interval and resets to zero on close.
	ConsecutiveTrips int
	// NextProbeIn is the time until the circuit becomes half-open and admits
	// a probe. Zero unless State is Open.
	NextProbeIn time.Duration
	// InFlight is the number of admitted calls that have not yet settled.
	InFlight int
	// InFlightLimit is the current bulkhead cap; 0 when unlimited. Under
	// [WithAdaptiveInFlight] it moves between the configured bounds, and
	// during a recovery ramp it climbs from the ramp's start.
	InFlightLimit int
	// Ramping is true while a [WithRecoveryRamp] is in progress: the circuit
	// has just closed and InFlightLimit is still climbing toward the ramp's
	// end. A low cap while Ramping is recovery working as designed.
	Ramping bool

	// Calls is every Do that reached the state goroutine, admitted or not.
	Calls uint64
	// Rejected is calls refused with ErrOpen or ErrProbeLimit; fn never ran.
	Rejected uint64
	// Shed is calls refused with ErrBulkhead; fn never ran.
	Shed uint64
	// Denied is calls vetoed by [WithAdmission]; fn never ran.
	Denied uint64
	// Admitted is calls for which fn ran to completion. A call still running
	// is in InFlight and not yet in Admitted.
	Admitted uint64
	// Successes is admitted calls whose outcome was a success per IsFailure.
	Successes uint64
	// Failures is admitted calls whose outcome was a failure per IsFailure.
	Failures uint64
	// Canceled is admitted calls that returned a context.Canceled that
	// IsFailure did not classify as a failure; they count for nothing.
	Canceled uint64
	// Trips is the total number of closed/half-open → open transitions.
	Trips uint64
}

// String renders the snapshot as one log line, for example:
//
//	breaker: name=db-fallback state=open trips=3(consecutive=2) calls=812 rejected=41 shed=2 denied=0 ok=760 fail=9 canceled=0 in_flight=3/64 next_probe_in=23s
//
// in_flight shows the cap after the slash only when one is set, followed by
// "(ramping)" during a recovery ramp; next_probe_in is present only while
// open.
func (s Stats) String() string {
	buf := fmt.Appendf(make([]byte, 0, 192), "breaker: name=%s state=%s trips=%d(consecutive=%d) calls=%d rejected=%d shed=%d denied=%d ok=%d fail=%d canceled=%d in_flight=%d",
		s.Name, s.State, s.Trips, s.ConsecutiveTrips, s.Calls, s.Rejected, s.Shed, s.Denied, s.Successes, s.Failures, s.Canceled, s.InFlight)
	if s.InFlightLimit > 0 {
		buf = fmt.Appendf(buf, "/%d", s.InFlightLimit)
	}
	if s.Ramping {
		buf = append(buf, "(ramping)"...)
	}
	if s.State == Open {
		buf = fmt.Appendf(buf, " next_probe_in=%s", s.NextProbeIn.Round(time.Millisecond))
	}
	return string(buf)
}

// Breaker is a channel-based circuit breaker. Create one with [New]; the zero
// value is not usable.
//
// A Breaker needs no teardown. Its state goroutine runs for as long as the
// Breaker is reachable and stops itself once it is not, the way a
// [time.Timer] is collected without Stop since Go 1.23. A breaker created at
// bootstrap simply lives for the process; one created for a shorter purpose is
// dropped like any other value. [Breaker.Stop] exists for callers who want the
// goroutine gone and the metric series removed at a moment of their choosing,
// such as tests; calling it is optional.
type Breaker struct {
	*core
}

// core is the machinery the handle points to. The state goroutine holds only
// the core, never the Breaker, which is what lets an unreachable Breaker be
// collected and its cleanup stop the goroutine.
type core struct {
	cfg config

	admit   chan token        // Do → loop: may I run? the loop replies on the token
	settle  chan settleMsg    // Do → loop: here is the outcome; the loop acks on the token
	inspect chan chan<- Stats // Stats → loop: snapshot please
	quit    chan struct{}     // stop → loop: exit
	stopped chan struct{}     // closed by the loop on exit

	// tokens is a free list of per-call channels. It is a plain buffered
	// channel used as a stack: acquire takes one if available, release puts
	// it back if there is room. Both are non-blocking, so the list is only an
	// optimisation; when it is empty a token is allocated, when it is full a
	// token is dropped for the GC.
	tokens chan token
}

// token is the channel a single call talks to the loop on. The loop first
// replies on it with the admission decision, then, after the call settles,
// acknowledges on it that the outcome was applied. It is buffered by one so
// the loop never blocks sending, and its element is pointer-free so the
// runtime allocates the header and buffer together in one object.
type token chan uint64

// Admission replies. A generation counter can never reach these values.
const (
	_replyOpen       = ^uint64(0)
	_replyProbeLimit = ^uint64(0) - 1
	_replyBulkhead   = ^uint64(0) - 2
)

// tokenPoolSize bounds the free list. It only needs to cover the number of
// calls that are typically in flight at once.
const _tokenPoolSize = 128

type outcome uint8

const (
	_outcomeSuccess outcome = iota
	_outcomeFailure
	_outcomeCanceled
	_outcomeDenied // admission veto: un-admit, count as denied, change nothing else
)

type settleMsg struct {
	gen     uint64
	out     outcome
	elapsed time.Duration // how long fn ran; drives the adaptive bulkhead
	tok     token         // acked with a send once the outcome is applied
}

// New starts a breaker and returns it. name identifies the
// dependency the breaker guards, for example "db-fallback": it appears in
// [Stats] and its log line and is the dependency label of the breaker's
// Prometheus metrics (see [Register]). Each name should belong to one live
// breaker at a time; two live breakers with the same name would merge their
// metrics. Options are applied in order; see [Option].
//
// If name is empty or any option is invalid, New returns an error that wraps
// [ErrInvalidOption] and describes every problem, and no goroutine is
// started. Nothing needs to be stopped afterwards; see [Breaker].
func New(name string, opts ...Option) (*Breaker, error) {
	cfg := defaultConfig()
	cfg.name = name
	var errs []error
	if name == "" {
		errs = append(errs, fmt.Errorf("%w: New: name must not be empty", ErrInvalidOption))
	}
	for _, opt := range opts {
		if err := opt(&cfg); err != nil {
			errs = append(errs, err)
		}
	}
	if cfg.maxInFlight > 0 && cfg.aimd != nil {
		errs = append(errs, fmt.Errorf("%w: WithMaxInFlight and WithAdaptiveInFlight are mutually exclusive", ErrInvalidOption))
	}
	if r := cfg.ramp; r != nil {
		switch {
		case cfg.maxInFlight > 0 && r.end > cfg.maxInFlight:
			errs = append(errs, fmt.Errorf("%w: WithRecoveryRamp end %d exceeds WithMaxInFlight %d", ErrInvalidOption, r.end, cfg.maxInFlight))
		case cfg.aimd != nil && (r.start < cfg.aimd.min || r.end > cfg.aimd.max):
			errs = append(errs, fmt.Errorf("%w: WithRecoveryRamp(%d, %d) outside WithAdaptiveInFlight bounds [%d, %d]", ErrInvalidOption, r.start, r.end, cfg.aimd.min, cfg.aimd.max))
		}
	}
	if err := errors.Join(errs...); err != nil {
		return nil, err
	}
	// Every breaker feeds the package metrics; Register decides whether those
	// are published.
	cfg.observers = append([]Observer{&metricsObserver{dep: cfg.name}}, cfg.observers...)
	if cfg.seed == [2]uint64{} {
		// The package-level generator is seeded by the runtime; it is used
		// once here to seed the loop's own generator and never for jitter.
		cfg.seed = [2]uint64{rand.Uint64(), rand.Uint64()}
	}

	c := &core{
		cfg:     cfg,
		admit:   make(chan token),
		settle:  make(chan settleMsg),
		inspect: make(chan chan<- Stats),
		quit:    make(chan struct{}),
		stopped: make(chan struct{}),
		tokens:  make(chan token, _tokenPoolSize),
	}
	// Started runs here, on the caller's goroutine, so that when New returns
	// every observer has seen the breaker; the state goroutine takes over
	// from there.
	for _, o := range cfg.observers {
		o.Started()
	}
	go c.run()
	b := &Breaker{core: c}
	// The goroutine references c, never b, so b becomes unreachable when the
	// caller drops it, and the cleanup stops the goroutine.
	runtime.AddCleanup(b, (*core).stop, c)
	return b, nil
}

// Do runs fn under the breaker and returns its result.
//
// If the circuit is open, Do returns [ErrOpen] without invoking fn. If it is
// half-open and MaxProbes probes are already in flight, Do returns
// [ErrProbeLimit] without invoking fn. If ctx is done before the call is
// admitted, Do returns ctx.Err(). After [Breaker.Stop], Do returns
// [ErrStopped]. In every other case fn is invoked exactly once and its result
// is returned unchanged; the breaker never rewrites fn's error. With
// [WithTimeout], the context fn receives is derived from ctx and expires after
// the configured duration.
//
// When Do returns, fn's outcome has been applied to the circuit: a
// subsequent [Breaker.Stats] or [Breaker.State] reflects it, any transition
// it caused has already been reported through the state-change hook, and any
// open deadline it started was computed against the clock as it read before
// Do returned.
//
// A panic in fn propagates to the caller after being recorded as a failure,
// so a panicking probe cannot leak a half-open probe slot.
//
// Do is a generic method and therefore cannot be part of an interface. To
// put the breaker behind an interface, define a non-generic adapter (for
// example over func(context.Context) error) and implement it with Do.
func (b *Breaker) Do[T any](ctx context.Context, fn func(context.Context) (T, error)) (T, error) {
	var zero T
	tok, gen, err := b.acquire(ctx)
	if err != nil {
		return zero, err
	}
	out := _outcomeFailure // a panic in fn leaves this set: counted as a failure
	start := b.cfg.now()
	defer func() { b.release(tok, gen, out, b.cfg.now().Sub(start)) }()
	if b.cfg.admission != nil {
		if err := b.cfg.admission(ctx); err != nil {
			out = _outcomeDenied
			return zero, err
		}
	}
	if b.cfg.timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, b.cfg.timeout)
		defer cancel()
	}
	v, err := fn(ctx)
	out = b.classify(err)
	return v, err
}

func (b *core) classify(err error) outcome {
	switch {
	case b.cfg.isFailure(err):
		return _outcomeFailure
	case err != nil && errors.Is(err, context.Canceled):
		return _outcomeCanceled
	default:
		return _outcomeSuccess
	}
}

// acquire asks the loop for admission and returns the call's token and the
// generation it was admitted under. The token must be handed to release.
//
// The select on ctx and stopped covers only the send. Once the send has
// succeeded the loop holds the reply channel and answers it without blocking,
// so the reply is awaited unconditionally. The receive must not be raced
// against ctx: if ctx fired between the loop admitting the call and the reply
// being read, the caller would return ctx.Err() while the loop has already
// counted the call as admitted and, while half-open, has handed it a probe
// slot that would never be released. Every later call would then be rejected
// with ErrProbeLimit and the circuit could never recover.
func (b *core) acquire(ctx context.Context) (token, uint64, error) {
	// select chooses uniformly among ready cases, so without this check an
	// already-cancelled ctx could still win admission against an idle loop.
	if err := ctx.Err(); err != nil {
		return nil, 0, err
	}
	tok := b.getToken()
	select {
	case b.admit <- tok:
	case <-ctx.Done():
		b.putToken(tok)
		return nil, 0, ctx.Err()
	case <-b.stopped:
		b.putToken(tok)
		return nil, 0, ErrStopped
	}
	switch r := <-tok; r {
	case _replyOpen:
		b.putToken(tok)
		return nil, 0, ErrOpen
	case _replyProbeLimit:
		b.putToken(tok)
		return nil, 0, ErrProbeLimit
	case _replyBulkhead:
		b.putToken(tok)
		return nil, 0, ErrBulkhead
	default:
		return tok, r, nil
	}
}

// getToken takes a token from the free list or allocates one.
func (b *core) getToken() token {
	select {
	case tok := <-b.tokens:
		return tok
	default:
		return make(token, 1)
	}
}

// putToken returns an empty token to the free list, or drops it if the list
// is full. Every caller has received whatever the loop sent on it, so it is
// empty.
func (b *core) putToken(tok token) {
	select {
	case b.tokens <- tok:
	default:
	}
}

// release reports the outcome of a call admitted under gen and waits for the
// loop to acknowledge on tok that it has been applied.
//
// The ack is what makes Do's "outcome applied on return" guarantee hold.
// Without it, Do would return once the loop had received the outcome but
// possibly before it had applied it. Nothing reachable through the API can
// observe that gap, because the loop handles messages in order and a
// subsequent Stats queues behind the settle; but anything holding the
// injected clock can. Advancing the clock right after Do returns would change
// the timestamp a pending trip's open deadline is computed against, and the
// circuit would open later than the caller expects. One extra channel round
// trip on a path that has just made a network call buys a guarantee that
// needs no barrier.
//
// If the breaker is stopped, the outcome is discarded.
func (b *core) release(tok token, gen uint64, out outcome, elapsed time.Duration) {
	select {
	case b.settle <- settleMsg{gen: gen, out: out, elapsed: elapsed, tok: tok}:
		<-tok
	case <-b.stopped:
	}
	b.putToken(tok)
}

// Stop shuts the state goroutine down now rather than when the Breaker is
// collected. It is optional: a Breaker that is simply dropped stops itself.
// Call it for deterministic teardown, in tests, or when the breaker's metric
// series should disappear immediately rather than after the next garbage
// collection. Idempotent and safe to call concurrently: the send on the
// unbuffered quit channel is raced against stopped, so exactly one caller
// wins and every other sees stopped close. Calls in flight complete and
// return fn's result; their outcomes are discarded. Afterwards Do returns
// [ErrStopped] and Stats the zero value.
func (b *Breaker) Stop() { b.core.stop() }

// State returns the current state, applying any due open → half-open
// transition first.
func (b *Breaker) State() State {
	return b.Stats().State
}

// Stats returns a snapshot of the breaker, applying any due open → half-open
// transition first.
func (b *Breaker) Stats() Stats {
	reply := make(chan Stats, 1)
	select {
	case b.inspect <- reply:
		return <-reply
	case <-b.stopped:
		return Stats{}
	}
}

// stop is Stop's implementation, on the core so the cleanup can run it
// without a handle.
func (b *core) stop() {
	select {
	case b.quit <- struct{}{}:
	case <-b.stopped:
	}
	<-b.stopped
}

// machine is the breaker's mutable state. It is instantiated exactly once, as
// a local variable of run, and no reference to it ever escapes that goroutine.
type machine struct {
	cfg config
	rng *rand.Rand

	state            State
	gen              uint64 // bumped on every transition; stamps admissions
	consecFailures   int    // closed: failures since the last success
	consecSuccesses  int    // half-open: successful probes since entering
	probesInFlight   int    // half-open: admitted, not yet settled
	inFlight         int    // admitted, not yet settled, in any state
	limit            int    // bulkhead cap; 0 = unlimited
	ramping          bool   // recovery ramp in progress
	openUntil        time.Time
	consecutiveTrips int

	calls, rejected, shed, denied, admitted, successes, failures, canceled, trips uint64
}

// run is the state goroutine. It exclusively owns every field of the machine:
// the state, the consecutive counters, the generation counter, openUntil,
// consecutiveTrips, the in-flight count, bulkhead cap and ramp flag, the
// cumulative totals and the RNG. Nothing outside this
// function reads or writes them; callers observe them only through the
// replies run sends. That single-owner discipline is what makes the absence
// of locking sound, and it is also why the state-change hook must not call
// back into the breaker.
func (b *core) run() {
	m := &machine{
		cfg:   b.cfg,
		rng:   rand.New(rand.NewPCG(b.cfg.seed[0], b.cfg.seed[1])),
		limit: b.cfg.maxInFlight,
	}
	if b.cfg.aimd != nil {
		m.limit = b.cfg.aimd.max
	}
	m.observeLoad() // initial cap; Started could not know it
	for {
		select {
		case tok := <-b.admit:
			tok <- m.handleAdmit()
		case msg := <-b.settle:
			m.handleSettle(msg.gen, msg.out, msg.elapsed)
			msg.tok <- 0
		case reply := <-b.inspect:
			reply <- m.snapshot()
		case <-b.quit:
			for _, o := range m.cfg.observers {
				o.Stopped()
			}
			close(b.stopped)
			return
		}
	}
}

func (m *machine) observeCall(r Result) {
	for _, o := range m.cfg.observers {
		o.Call(r)
	}
}

func (m *machine) observeLoad() {
	for _, o := range m.cfg.observers {
		o.Load(m.inFlight, m.limit, m.ramping)
	}
}

// expire performs the lazy open → half-open transition if the open interval
// has elapsed. It is called at the top of every admit and inspect.
func (m *machine) expire(now time.Time) {
	if m.state == Open && !now.Before(m.openUntil) {
		m.transition(HalfOpen)
	}
}

// handleAdmit decides admission and returns the reply to send on the token:
// the current generation, or one of the rejection codes. Checks run from the
// most informative rejection to the least: an open circuit says so before the
// bulkhead does.
func (m *machine) handleAdmit() uint64 {
	m.expire(m.cfg.now())
	m.calls++
	switch m.state {
	case Closed:
		return m.admitOrShed()
	case HalfOpen:
		if m.probesInFlight >= m.cfg.maxProbes {
			m.rejected++
			m.observeCall(Rejected)
			return _replyProbeLimit
		}
		reply := m.admitOrShed()
		if reply != _replyBulkhead {
			m.probesInFlight++
		}
		return reply
	case Open:
		m.rejected++
		m.observeCall(Rejected)
		return _replyOpen
	}
	// Unreachable: state only ever holds the three values above. Rejecting is
	// the safe answer if that invariant were ever broken.
	m.rejected++
	m.observeCall(Rejected)
	return _replyOpen
}

// admitOrShed applies the bulkhead to a call the state machine would admit.
// The call joins inFlight here and is counted as admitted only when it
// settles, so Admitted stays monotonic even if the admission veto later
// refuses it.
func (m *machine) admitOrShed() uint64 {
	if m.limit > 0 && m.inFlight >= m.limit {
		m.shed++
		m.observeCall(Shed)
		return _replyBulkhead
	}
	m.inFlight++
	m.observeLoad()
	return m.gen
}

func (m *machine) handleSettle(gen uint64, out outcome, elapsed time.Duration) {
	defer m.observeLoad() // after any transition below, so the cap it set is seen
	m.inFlight--          // every admitted call settles exactly once, stale or not
	if out == _outcomeDenied {
		// The veto refused the call after the circuit let it through: it never
		// ran, so it is denied rather than admitted, and nothing about the
		// backend was learned.
		m.denied++
		m.observeCall(Denied)
		if gen == m.gen && m.state == HalfOpen {
			m.probesInFlight--
		}
		return
	}
	m.adapt(out, elapsed)
	m.admitted++
	switch out {
	case _outcomeSuccess:
		m.successes++
		m.observeCall(Success)
	case _outcomeFailure:
		m.failures++
		m.observeCall(Failure)
	case _outcomeCanceled:
		m.canceled++
		m.observeCall(Canceled)
	case _outcomeDenied:
		// Counted before the stale check; a denial says nothing about the backend.
	}
	// A stale outcome belongs to a call admitted before the last transition.
	// It has been counted above but must not drive the state machine: a slow
	// success from before a trip says nothing about the backend now, and
	// acting on it would close a circuit that has just, correctly, opened.
	if gen != m.gen {
		return
	}
	switch m.state {
	case Closed:
		switch out {
		case _outcomeSuccess:
			m.consecFailures = 0
			m.rampStep()
		case _outcomeFailure:
			m.consecFailures++
			if m.consecFailures >= m.cfg.failureThreshold {
				m.transition(Open)
			}
		case _outcomeCanceled:
			// Neutral: the caller gave up, which says nothing about the
			// backend. The failure run is neither extended nor reset.
		case _outcomeDenied:
			// Never reached: handled at the top of handleSettle.
		}
	case HalfOpen:
		m.probesInFlight--
		switch out {
		case _outcomeSuccess:
			m.consecSuccesses++
			if m.consecSuccesses >= m.cfg.successThreshold {
				m.transition(Closed)
			}
		case _outcomeFailure:
			m.transition(Open)
		case _outcomeCanceled:
			// Neutral: the probe slot is freed above and the success run is
			// left as it was.
		case _outcomeDenied:
			// Never reached: handled at the top of handleSettle.
		}
	case Open:
		// Unreachable: entering Open bumps gen, so every in-flight call is
		// stale and returned above.
	}
}

// adapt applies the AIMD rule to the bulkhead cap: +1 for a success within
// target, halve for a failure or a slow call, unchanged for a cancellation.
func (m *machine) adapt(out outcome, elapsed time.Duration) {
	a := m.cfg.aimd
	if a == nil {
		return
	}
	prev := m.limit
	defer func() {
		// Under AIMD the ramp is only a seed: it is over once AIMD has grown
		// the cap to the ramp's end, or lowered it, which means backoff.
		if m.ramping && (m.limit < prev || m.limit >= m.cfg.ramp.end) {
			m.ramping = false
		}
	}()
	switch out {
	case _outcomeSuccess:
		if elapsed <= a.target {
			m.limit = min(m.limit+1, a.max)
		} else {
			m.limit = max(m.limit/2, a.min)
		}
	case _outcomeFailure:
		m.limit = max(m.limit/2, a.min)
	case _outcomeCanceled:
		// Neutral: the caller gave up; that says nothing about capacity.
	case _outcomeDenied:
		// Never reached: handleSettle returns before adapt for denials.
	}
}

// steadyLimit is the in-flight cap outside a ramp: the static bulkhead, or 0
// for unlimited. Under AIMD the cap is whatever AIMD last decided.
func (m *machine) steadyLimit() int {
	if m.cfg.aimd != nil {
		return m.limit
	}
	return m.cfg.maxInFlight
}

// rampStart begins a recovery ramp on close.
func (m *machine) rampStart() {
	r := m.cfg.ramp
	if r == nil {
		return
	}
	m.ramping = true
	m.limit = r.start
	if a := m.cfg.aimd; a != nil {
		m.limit = min(max(m.limit, a.min), a.max)
	}
}

// rampStep grows a static ramp by one for a success while closed and ends it
// at the ramp's end. Under AIMD growth belongs to adapt.
func (m *machine) rampStep() {
	if !m.ramping || m.cfg.aimd != nil {
		return
	}
	m.limit++
	if m.limit >= m.cfg.ramp.end {
		m.ramping = false
		m.limit = m.steadyLimit()
	}
}

// rampAbort ends a ramp on any transition away from closed.
func (m *machine) rampAbort() {
	if m.ramping {
		m.ramping = false
		m.limit = m.steadyLimit()
	}
}

// transition moves to state to, bumps the generation so in-flight calls
// become stale, resets the per-state counters, and notifies the state-change
// hook and then the observers, once the machine is fully consistent.
func (m *machine) transition(to State) {
	from := m.state
	m.state = to
	m.gen++
	m.consecFailures = 0
	m.consecSuccesses = 0
	m.probesInFlight = 0
	switch to {
	case Closed:
		m.consecutiveTrips = 0
		m.rampStart()
	case Open:
		m.trips++
		m.consecutiveTrips++
		m.openUntil = m.cfg.now().Add(m.openInterval())
		m.rampAbort()
	case HalfOpen:
		// Nothing beyond the common resets: the probe budget starts fresh
		// and the open deadline is left for Stats to stop reporting.
	}
	if m.cfg.onStateChange != nil {
		m.cfg.onStateChange(from, to)
	}
	var openUntil time.Time
	if to == Open {
		openUntil = m.openUntil
	}
	for _, o := range m.cfg.observers {
		o.Transition(from, to, m.consecutiveTrips, openUntil)
	}
}

// openInterval computes the open interval for the current consecutiveTrips:
// OpenBase doubled per consecutive trip, capped at OpenMax, then jittered by
// ±OpenJitter. It draws from the RNG once and is called once per trip, so a
// given open period has one fixed deadline.
func (m *machine) openInterval() time.Duration {
	d := m.cfg.openBase
	for range m.consecutiveTrips - 1 {
		if d >= m.cfg.openMax/2 {
			d = m.cfg.openMax
			break
		}
		d *= 2
	}
	d = min(d, m.cfg.openMax)
	if j := m.cfg.openJitter; j > 0 {
		d = time.Duration(float64(d) * (1 + j*(2*m.rng.Float64()-1)))
	}
	return max(d, 0)
}

func (m *machine) snapshot() Stats {
	now := m.cfg.now()
	m.expire(now)
	s := Stats{
		Name:             m.cfg.name,
		State:            m.state,
		ConsecutiveTrips: m.consecutiveTrips,
		InFlight:         m.inFlight,
		InFlightLimit:    m.limit,
		Ramping:          m.ramping,
		Calls:            m.calls,
		Rejected:         m.rejected,
		Shed:             m.shed,
		Denied:           m.denied,
		Admitted:         m.admitted,
		Successes:        m.successes,
		Failures:         m.failures,
		Canceled:         m.canceled,
		Trips:            m.trips,
	}
	if m.state == Open {
		s.NextProbeIn = m.openUntil.Sub(now)
	}
	return s
}
